"""

This module contains the component responsible for consuming messages
from the broker, processing the messages and keeping the broker connections
up and running.


* :meth:`~Consumer.start` is an infinite loop, which only iterates
  again if the connection is lost. For each iteration (at start, or if the
  connection is lost) it calls :meth:`~Consumer.reset_connection`,
  and starts the consumer by calling :meth:`~Consumer.consume_messages`.

* :meth:`~Consumer.reset_connection`, clears the internal queues,
  establishes a new connection to the broker, sets up the task
  consumer (+ QoS), and the broadcast remote control command consumer.

  Also if events are enabled it configures the event dispatcher and starts
  up the hartbeat thread.

* Finally it can consume messages. :meth:`~Consumer.consume_messages`
  is simply an infinite loop waiting for events on the AMQP channels.

  Both the task consumer and the broadcast consumer uses the same
  callback: :meth:`~Consumer.receive_message`.

* So for each message received the :meth:`~Consumer.receive_message`
  method is called, this checks the payload of the message for either
  a `task` key or a `control` key.

  If the message is a task, it verifies the validity of the message
  converts it to a :class:`celery.worker.job.TaskRequest`, and sends
  it to :meth:`~Consumer.on_task`.

  If the message is a control command the message is passed to
  :meth:`~Consumer.on_control`, which in turn dispatches
  the control command using the control dispatcher.

  It also tries to handle malformed or invalid messages properly,
  so the worker doesn't choke on them and die. Any invalid messages
  are acknowledged immediately and logged, so the message is not resent
  again, and again.

* If the task has an ETA/countdown, the task is moved to the `eta_schedule`
  so the :class:`timer2.Timer` can schedule it at its
  deadline. Tasks without an eta are moved immediately to the `ready_queue`,
  so they can be picked up by the :class:`~celery.worker.mediator.Mediator`
  to be sent to the pool.

* When a task with an ETA is received the QoS prefetch count is also
  incremented, so another message can be reserved. When the ETA is met
  the prefetch count is decremented again, though this cannot happen
  immediately because amqplib doesn't support doing broker requests
  across threads. Instead the current prefetch count is kept as a
  shared counter, so as soon as  :meth:`~Consumer.consume_messages`
  detects that the value has changed it will send out the actual
  QoS event to the broker.

* Notice that when the connection is lost all internal queues are cleared
  because we can no longer ack the messages reserved in memory.
  Hoever, this is not dangerous as the broker will resend them
  to another worker when the channel is closed.

* **WARNING**: :meth:`~Consumer.stop` does not close the connection!
  This is because some pre-acked messages may be in processing,
  and they need to be finished before the channel is closed.
  For celeryd this means the pool must finish the tasks it has acked
  early, *then* close the connection.

"""

from __future__ import generators

import socket
import sys
import threading
import traceback
import warnings

from celery.app import app_or_default
from celery.datastructures import AttributeDict
from celery.exceptions import NotRegistered
from celery.utils import noop
from celery.utils import timer2
from celery.utils.encoding import safe_repr
from celery.worker import state
from celery.worker.job import TaskRequest, InvalidTaskError
from celery.worker.control.registry import Panel
from celery.worker.heartbeat import Heart

RUN = 0x1
CLOSE = 0x2

#: Prefetch count can't exceed short.
PREFETCH_COUNT_MAX = 0xFFFF


class QoS(object):
    """Quality of Service for Channel.

    For thread-safe increment/decrement of a channels prefetch count value.

    :param consumer: A :class:`kombu.messaging.Consumer` instance.
    :param initial_value: Initial prefetch count value.
    :param logger: Logger used to log debug messages.

    """
    prev = None

    def __init__(self, consumer, initial_value, logger):
        self.consumer = consumer
        self.logger = logger
        self._mutex = threading.RLock()
        self.value = initial_value

    def increment(self, n=1):
        """Increment the current prefetch count value by one."""
        self._mutex.acquire()
        try:
            if self.value:
                new_value = self.value + max(n, 0)
                self.value = self.set(new_value)
            return self.value
        finally:
            self._mutex.release()

    def _sub(self, n=1):
        assert self.value - n > 1
        self.value -= n

    def decrement(self, n=1):
        """Decrement the current prefetch count value by one."""
        self._mutex.acquire()
        try:
            if self.value:
                self._sub(n)
                self.set(self.value)
            return self.value
        finally:
            self._mutex.release()

    def decrement_eventually(self, n=1):
        """Decrement the value, but do not update the qos.

        The MainThread will be responsible for calling :meth:`update`
        when necessary.

        """
        self._mutex.acquire()
        try:
            if self.value:
                self._sub(n)
        finally:
            self._mutex.release()

    def set(self, pcount):
        """Set channel prefetch_count setting."""
        if pcount != self.prev:
            new_value = pcount
            if pcount > PREFETCH_COUNT_MAX:
                self.logger.warning(
                    "QoS: Disabled: prefetch_count exceeds %r" % (
                        PREFETCH_COUNT_MAX, ))
                new_value = 0
            self.logger.debug("basic.qos: prefetch_count->%s" % new_value)
            self.consumer.qos(prefetch_count=new_value)
            self.prev = pcount
        return pcount

    def update(self):
        """Update prefetch count with current value."""
        self._mutex.acquire()
        try:
            return self.set(self.value)
        finally:
            self._mutex.release()


class Consumer(object):
    """Listen for messages received from the broker and
    move them the the ready queue for task processing.

    :param ready_queue: See :attr:`ready_queue`.
    :param eta_schedule: See :attr:`eta_schedule`.

    .. attribute:: ready_queue

        The queue that holds tasks ready for immediate processing.

    .. attribute:: eta_schedule

        Scheduler for paused tasks. Reasons for being paused include
        a countdown/eta or that it's waiting for retry.

    .. attribute:: send_events

        Is events enabled?

    .. attribute:: init_callback

        Callback to be called the first time the connection is active.

    .. attribute:: hostname

        Current hostname. Defaults to the system hostname.

    .. attribute:: initial_prefetch_count

        Initial QoS prefetch count for the task channel.

    .. attribute:: control_dispatch

        Control command dispatcher.
        See :class:`celery.worker.control.ControlDispatch`.

    .. attribute:: event_dispatcher

        See :class:`celery.events.EventDispatcher`.

    .. attribute:: hart

        :class:`~celery.worker.heartbeat.Heart` sending out heart beats
        if events enabled.

    .. attribute:: logger

        The logger used.

    """
    _state = None

    def __init__(self, ready_queue, eta_schedule, logger,
            init_callback=noop, send_events=False, hostname=None,
            initial_prefetch_count=2, pool=None, app=None,
            priority_timer=None):
        self.app = app_or_default(app)
        self.connection = None
        self.task_consumer = None
        self.broadcast_consumer = None
        self.ready_queue = ready_queue
        self.eta_schedule = eta_schedule
        self.send_events = send_events
        self.init_callback = init_callback
        self.logger = logger
        self.hostname = hostname or socket.gethostname()
        self.initial_prefetch_count = initial_prefetch_count
        self.event_dispatcher = None
        self.heart = None
        self.pool = pool
        self.priority_timer = priority_timer or timer2.Timer()
        pidbox_state = AttributeDict(app=self.app,
                                     logger=logger,
                                     hostname=self.hostname,
                                     listener=self,     # pre 2.2
                                     consumer=self)
        self.pidbox_node = self.app.control.mailbox.Node(self.hostname,
                                                         state=pidbox_state,
                                                         handlers=Panel.data)
        conninfo = self.app.broker_connection()
        self.connection_errors = conninfo.connection_errors
        self.channel_errors = conninfo.channel_errors

    def start(self):
        """Start the consumer.

        If the connection is lost, it tries to re-establish the connection
        and restarts consuming messages.

        """

        self.init_callback(self)

        while self._state != CLOSE:
            try:
                self.reset_connection()
                self.consume_messages()
            except self.connection_errors:
                self.logger.error("Consumer: Connection to broker lost."
                                + " Trying to re-establish connection...",
                                exc_info=sys.exc_info())

    def consume_messages(self):
        """Consume messages forever (or until an exception is raised)."""
        self.logger.debug("Consumer: Starting message consumer...")
        self.task_consumer.consume()
        self.logger.debug("Consumer: Ready to accept tasks!")

        while self._state != CLOSE and self.connection:
            if self.qos.prev != self.qos.value:
                self.qos.update()
            try:
                self.connection.drain_events(timeout=1)
            except socket.timeout:
                pass
            except socket.error:
                if self._state != CLOSE:
                    raise

    def on_task(self, task):
        """Handle received task.

        If the task has an `eta` we enter it into the ETA schedule,
        otherwise we move it the ready queue for immediate processing.

        """

        if task.revoked():
            return

        self.logger.info("Got task from broker: %s" % (task.shortinfo(), ))

        self.event_dispatcher.send("task-received", uuid=task.task_id,
                name=task.task_name, args=safe_repr(task.args),
                kwargs=safe_repr(task.kwargs), retries=task.retries,
                eta=task.eta and task.eta.isoformat(),
                expires=task.expires and task.expires.isoformat())

        if task.eta:
            try:
                eta = timer2.to_timestamp(task.eta)
            except OverflowError, exc:
                self.logger.error(
                    "Couldn't convert eta %s to timestamp: %r. Task: %r" % (
                        task.eta, exc, task.info(safe=True)),
                    exc_info=sys.exc_info())
                task.acknowledge()
            else:
                self.qos.increment()
                self.eta_schedule.apply_at(eta,
                                           self.apply_eta_task, (task, ))
        else:
            state.task_reserved(task)
            self.ready_queue.put(task)

    def on_control(self, body, message):
        try:
            self.pidbox_node.handle_message(body, message)
        except KeyError, exc:
            self.logger.error("No such control command: %s" % exc)
        except Exception, exc:
            self.logger.error(
                "Error occurred while handling control command: %r\n%r" % (
                    exc, traceback.format_exc()), exc_info=sys.exc_info())
            self.reset_pidbox_node()

    def apply_eta_task(self, task):
        state.task_reserved(task)
        self.ready_queue.put(task)
        self.qos.decrement_eventually()

    def receive_message(self, body, message):
        """The callback called when a new message is received. """

        # Handle task
        if body.get("task"):
            def ack():
                try:
                    message.ack()
                except self.connection_errors + (AttributeError, ), exc:
                    self.logger.critical(
                            "Couldn't ack %r: message:%r reason:%r" % (
                                message.delivery_tag, safe_repr(body), exc))

            try:
                task = TaskRequest.from_message(message, body, ack,
                                                app=self.app,
                                                logger=self.logger,
                                                hostname=self.hostname,
                                                eventer=self.event_dispatcher)
            except NotRegistered, exc:
                self.logger.error("Unknown task ignored: %r Body->%r" % (
                        exc, safe_repr(body)), exc_info=sys.exc_info())
                message.ack()
            except InvalidTaskError, exc:
                self.logger.error("Invalid task ignored: %s: %s" % (
                        str(exc), safe_repr(body)), exc_info=sys.exc_info())
                message.ack()
            else:
                self.on_task(task)
            return

        warnings.warn(RuntimeWarning(
            "Received and deleted unknown message. Wrong destination?!? \
             the message was: %s" % body))
        message.ack()

    def maybe_conn_error(self, fun):
        try:
            fun()
        except (AttributeError, ) + \
                self.connection_errors + \
                self.channel_errors:
            pass

    def close_connection(self):
        if self.task_consumer:
            self.logger.debug("Consumer: " "Closing consumer channel...")
            self.task_consumer = \
                    self.maybe_conn_error(self.task_consumer.close)
        if self.broadcast_consumer:
            self.logger.debug("CarrotListener: Closing broadcast channel...")
            self.broadcast_consumer = \
                self.maybe_conn_error(self.broadcast_consumer.channel.close)

        if self.connection:
            self.logger.debug("Consumer: " "Closing connection to broker...")
            self.connection = self.maybe_conn_error(self.connection.close)

    def stop_consumers(self, close=True):
        """Stop consuming."""
        if not self._state == RUN:
            return

        if self.heart:
            self.logger.debug("Heart: Going into cardiac arrest...")
            self.heart = self.heart.stop()

        self.logger.debug("TaskConsumer: Cancelling consumers...")
        if self.task_consumer:
            self.maybe_conn_error(self.task_consumer.cancel)

        if self.event_dispatcher:
            self.logger.debug("EventDispatcher: Shutting down...")
            self.event_dispatcher = \
                    self.maybe_conn_error(self.event_dispatcher.close)

        self.logger.debug("BroadcastConsumer: Cancelling consumer...")
        if self.broadcast_consumer:
            self.maybe_conn_error(self.broadcast_consumer.cancel)

        if close:
            self.close_connection()

    def on_decode_error(self, message, exc):
        """Callback called if the message had decoding errors.

        :param message: The message with errors.
        :param exc: The original exception instance.

        """
        self.logger.critical(
            "Can't decode message body: %r (type:%r encoding:%r raw:%r')" % (
                    exc, message.content_type, message.content_encoding,
                    safe_repr(message.body)))
        message.ack()

    def reset_pidbox_node(self):
        if self.pidbox_node.channel:
            try:
                self.pidbox_node.channel.close()
            except self.connection_errors + self.channel_errors:
                pass

        if self.pool.is_green:
            return self.pool.spawn_n(self._green_pidbox_node)
        self.pidbox_node.channel = self.connection.channel()
        self.broadcast_consumer = self.pidbox_node.listen(
                                        callback=self.on_control)
        self.broadcast_consumer.consume()

    def _green_pidbox_node(self):
        conn = self._open_connection()
        self.pidbox_node.channel = conn.channel()
        self.broadcast_consumer = self.pidbox_node.listen(
                                        callback=self.on_control)
        self.broadcast_consumer.consume()

        try:
            while self.connection:  # main connection still open?
                conn.drain_events()
        finally:
            conn.close()

    def reset_connection(self):
        """Re-establish connection and set up consumers."""
        self.logger.debug(
                "Consumer: Re-establishing connection to the broker...")
        self.stop_consumers()

        # Clear internal queues.
        self.ready_queue.clear()
        self.eta_schedule.clear()

        self.connection = self._open_connection()
        self.logger.debug("Consumer: Connection Established.")
        self.task_consumer = self.app.amqp.get_task_consumer(self.connection,
                                    on_decode_error=self.on_decode_error)
        # QoS: Reset prefetch window.
        self.qos = QoS(self.task_consumer,
                       self.initial_prefetch_count, self.logger)
        self.qos.update()                   # enable prefetch_count

        self.task_consumer.register_callback(self.receive_message)

        # Pidbox
        self.reset_pidbox_node()

        # Flush events sent while connection was down.
        prev_event_dispatcher = self.event_dispatcher
        self.event_dispatcher = self.app.events.Dispatcher(self.connection,
                                                hostname=self.hostname,
                                                enabled=self.send_events)
        if prev_event_dispatcher:
            self.event_dispatcher.copy_buffer(prev_event_dispatcher)
            self.event_dispatcher.flush()

        self.restart_heartbeat()

        self._state = RUN

    def restart_heartbeat(self):
        self.heart = Heart(self.priority_timer, self.event_dispatcher)
        self.heart.start()

    def _open_connection(self):
        """Open connection.  May retry opening the connection if configuration
        allows that."""

        def _connection_error_handler(exc, interval):
            """Callback handler for connection errors."""
            self.logger.error("Consumer: Connection Error: %s. " % exc
                     + "Trying again in %d seconds..." % interval)

        conn = self.app.broker_connection()
        if not self.app.conf.BROKER_CONNECTION_RETRY:
            conn.connect()
            return conn

        return conn.ensure_connection(_connection_error_handler,
                    self.app.conf.BROKER_CONNECTION_MAX_RETRIES)

    def stop(self):
        """Stop consuming.

        Does not close connection.

        """
        self._state = CLOSE
        self.logger.debug("Consumer: Stopping consumers...")
        self.stop_consumers(close=False)

    @property
    def info(self):
        conninfo = {}
        if self.connection:
            conninfo = self.connection.info()
            conninfo.pop("password", None)  # don't send password.
        return {"broker": conninfo,
                "prefetch_count": self.qos.value}
