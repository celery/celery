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
  so they can be picked up by the :class:`~celery.worker.controllers.Mediator`
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
import traceback
import warnings

from celery.app import app_or_default
from celery.datastructures import AttributeDict, SharedCounter
from celery.exceptions import NotRegistered
from celery.utils import noop
from celery.utils.timer2 import to_timestamp
from celery.worker import state
from celery.worker.job import TaskRequest, InvalidTaskError
from celery.worker.control.registry import Panel
from celery.worker.heartbeat import Heart

RUN = 0x1
CLOSE = 0x2


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
        self.value = SharedCounter(initial_value)

    def increment(self):
        """Increment the current prefetch count value by one."""
        if int(self.value):
            return self.set(self.value.increment())

    def decrement(self):
        """Decrement the current prefetch count value by one."""
        if int(self.value):
            return self.set(self.value.decrement())

    def decrement_eventually(self):
        """Decrement the value, but do not update the qos.

        The MainThread will be responsible for calling :meth:`update`
        when necessary.

        """
        self.value.decrement()

    def set(self, pcount):
        """Set channel prefetch_count setting."""
        self.logger.debug("basic.qos: prefetch_count->%s" % pcount)
        self.consumer.qos(prefetch_count=pcount)
        self.prev = pcount
        return pcount

    def update(self):
        """Update prefetch count with current value."""
        return self.set(self.next)

    @property
    def next(self):
        return int(self.value)


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
            initial_prefetch_count=2, pool=None, queues=None, app=None):

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
        pidbox_state = AttributeDict(app=self.app,
                                     logger=logger,
                                     hostname=self.hostname,
                                     listener=self,     # pre 2.2
                                     consumer=self)
        self.pidbox_node = self.app.control.mailbox.Node(self.hostname,
                                                         state=pidbox_state,
                                                         handlers=Panel.data)
        self.connection_errors = \
                self.app.broker_connection().connection_errors
        self.queues = queues

    def start(self):
        """Start the consumer.

        If the connection is lost, it tries to re-establish the connection
        and restarts consuming messages.

        """

        self.init_callback(self)

        while 1:
            self.reset_connection()
            try:
                self.consume_messages()
            except self.connection_errors:
                self.logger.error("Consumer: Connection to broker lost."
                                + " Trying to re-establish connection...")

    def consume_messages(self):
        """Consume messages forever (or until an exception is raised)."""
        self.logger.debug("Consumer: Starting message consumer...")
        self.task_consumer.consume()
        self.broadcast_consumer.consume()
        self.logger.debug("Consumer: Ready to accept tasks!")

        while 1:
            if not self.connection:
                break
            if self.qos.prev != self.qos.next:
                self.qos.update()
            self.connection.drain_events()

    def on_task(self, task):
        """Handle received task.

        If the task has an `eta` we enter it into the ETA schedule,
        otherwise we move it the ready queue for immediate processing.

        """

        if task.revoked():
            return

        self.logger.info("Got task from broker: %s" % (task.shortinfo(), ))

        self.event_dispatcher.send("task-received", uuid=task.task_id,
                name=task.task_name, args=repr(task.args),
                kwargs=repr(task.kwargs), retries=task.retries,
                eta=task.eta and task.eta.isoformat(),
                expires=task.expires and task.expires.isoformat())

        if task.eta:
            try:
                eta = to_timestamp(task.eta)
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

    def on_control(self, message, message_data):
        try:
            self.pidbox_node.handle_message(message, message_data)
        except KeyError, exc:
            self.logger.error("No such control command: %s" % exc)
        except Exception, exc:
            self.logger.error(
                "Error occurred while handling control command: %r\n%r" % (
                    exc, traceback.format_exc()))

    def apply_eta_task(self, task):
        state.task_reserved(task)
        self.ready_queue.put(task)
        self.qos.decrement_eventually()

    def receive_message(self, message_data, message):
        """The callback called when a new message is received. """

        # Handle task
        if message_data.get("task"):
            def ack():
                try:
                    message.ack()
                except self.connection_errors, exc:
                    self.logger.critical(
                            "Couldn't ack %r: message:%r reason:%r" % (
                                message.delivery_tag, message_data, exc))

            try:
                task = TaskRequest.from_message(message, message_data, ack,
                                                app=self.app,
                                                logger=self.logger,
                                                hostname=self.hostname,
                                                eventer=self.event_dispatcher)
            except NotRegistered, exc:
                self.logger.error("Unknown task ignored: %s: %s" % (
                        str(exc), message_data), exc_info=sys.exc_info())
                message.ack()
            except InvalidTaskError, exc:
                self.logger.error("Invalid task ignored: %s: %s" % (
                        str(exc), message_data), exc_info=sys.exc_info())
                message.ack()
            else:
                self.on_task(task)
            return

        warnings.warn(RuntimeWarning(
            "Received and deleted unknown message. Wrong destination?!? \
             the message was: %s" % message_data))
        message.ack()

    def maybe_conn_error(self, fun):
        try:
            fun()
        except (AttributeError, ) + \
                self.connection_errors + \
                self.channel_errors:
            pass

    def close_connection(self):
        self.logger.debug("Consumer: "
                          "Closing consumer channel...")
        if self.task_consumer:
            self.task_consumer = \
                    self.maybe_conn_error(self.task_consumer.close)
        self.logger.debug("Consumer: "
                          "Closing connection to broker...")

        self.logger.debug("CarrotListener: Closing broadcast channel...")
        if self.broadcast_consumer:
            self.broadcast_consumer = \
                self.maybe_conn_error(self.broadcast_consumer.channel.close)

        if self.connection:
            self.connection = self.maybe_conn_error(self.connection.close)

    def stop_consumers(self, close=True):
        """Stop consuming."""
        if not self._state == RUN:
            return
        self._state = CLOSE

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
        self.logger.critical("Message decoding error: %s "
                             "(type:%s encoding:%s raw:'%s')" % (
                                exc, message.content_type,
                                message.content_encoding, message.body))
        message.ack()

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
                                    queues=self.queues,
                                    on_decode_error=self.on_decode_error)
        # QoS: Reset prefetch window.
        self.qos = QoS(self.task_consumer,
                       self.initial_prefetch_count, self.logger)
        self.qos.update()                   # enable prefetch_count

        self.task_consumer.register_callback(self.receive_message)

        self.pidbox_node.channel = self.connection.channel()
        self.broadcast_consumer = self.pidbox_node.listen(
                                        callback=self.on_control)

        # Flush events sent while connection was down.
        if self.event_dispatcher:
            self.event_dispatcher.flush()
        self.event_dispatcher = self.app.events.Dispatcher(self.connection,
                                                hostname=self.hostname,
                                                enabled=self.send_events)
        self.restart_heartbeat()

        self._state = RUN

    def restart_heartbeat(self):
        self.heart = Heart(self.event_dispatcher)
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
        self.logger.debug("Consumer: Stopping consumers...")
        self.stop_consumers(close=False)

    @property
    def info(self):
        conninfo = {}
        if self.connection:
            conninfo = self.app.amqp.get_broker_info(self.connection)
        return {"broker": conninfo,
                "prefetch_count": self.qos.next}
