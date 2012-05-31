# -*- coding: utf-8 -*-
"""
celery.worker.consumer
~~~~~~~~~~~~~~~~~~~~~~

This module contains the component responsible for consuming messages
from the broker, processing the messages and keeping the broker connections
up and running.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.


* :meth:`~Consumer.start` is an infinite loop, which only iterates
  again if the connection is lost. For each iteration (at start, or if the
  connection is lost) it calls :meth:`~Consumer.reset_connection`,
  and starts the consumer by calling :meth:`~Consumer.consume_messages`.

* :meth:`~Consumer.reset_connection`, clears the internal queues,
  establishes a new connection to the broker, sets up the task
  consumer (+ QoS), and the broadcast remote control command consumer.

  Also if events are enabled it configures the event dispatcher and starts
  up the heartbeat thread.

* Finally it can consume messages. :meth:`~Consumer.consume_messages`
  is simply an infinite loop waiting for events on the AMQP channels.

  Both the task consumer and the broadcast consumer uses the same
  callback: :meth:`~Consumer.receive_message`.

* So for each message received the :meth:`~Consumer.receive_message`
  method is called, this checks the payload of the message for either
  a `task` key or a `control` key.

  If the message is a task, it verifies the validity of the message
  converts it to a :class:`celery.worker.job.Request`, and sends
  it to :meth:`~Consumer.on_task`.

  If the message is a control command the message is passed to
  :meth:`~Consumer.on_control`, which in turn dispatches
  the control command using the control dispatcher.

  It also tries to handle malformed or invalid messages properly,
  so the worker doesn't choke on them and die. Any invalid messages
  are acknowledged immediately and logged, so the message is not resent
  again, and again.

* If the task has an ETA/countdown, the task is moved to the `timer`
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
  However, this is not dangerous as the broker will resend them
  to another worker when the channel is closed.

* **WARNING**: :meth:`~Consumer.stop` does not close the connection!
  This is because some pre-acked messages may be in processing,
  and they need to be finished before the channel is closed.
  For celeryd this means the pool must finish the tasks it has acked
  early, *then* close the connection.

"""
from __future__ import absolute_import
from __future__ import with_statement

import logging
import socket
import threading

from time import sleep
from Queue import Empty

from kombu.utils.encoding import safe_repr
from kombu.utils.eventio import READ, WRITE, ERR

from celery.app import app_or_default
from celery.datastructures import AttributeDict
from celery.exceptions import InvalidTaskError, SystemTerminate
from celery.task.trace import build_tracer
from celery.utils import timer2
from celery.utils.functional import noop
from celery.utils.log import get_logger
from celery.utils import text

from . import state
from .abstract import StartStopComponent
from .control import Panel
from .heartbeat import Heart

RUN = 0x1
CLOSE = 0x2

#: Prefetch count can't exceed short.
PREFETCH_COUNT_MAX = 0xFFFF

UNKNOWN_FORMAT = """\
Received and deleted unknown message. Wrong destination?!?

The full contents of the message body was: %s
"""
#: Error message for when an unregistered task is received.
UNKNOWN_TASK_ERROR = """\
Received unregistered task of type %s.
The message has been ignored and discarded.

Did you remember to import the module containing this task?
Or maybe you are using relative imports?
Please see http://bit.ly/gLye1c for more information.

The full contents of the message body was:
%s
"""

#: Error message for when an invalid task message is received.
INVALID_TASK_ERROR = """\
Received invalid task message: %s
The message has been ignored and discarded.

Please ensure your message conforms to the task
message protocol as described here: http://bit.ly/hYj41y

The full contents of the message body was:
%s
"""

MESSAGE_REPORT_FMT = """\
body: %s {content_type:%s content_encoding:%s delivery_info:%s}\
"""


RETRY_CONNECTION = """\
Consumer: Connection to broker lost. \
Trying to re-establish the connection...\
"""

task_reserved = state.task_reserved

logger = get_logger(__name__)
info, warn, error, crit = (logger.info, logger.warn,
                           logger.error, logger.critical)


def debug(msg, *args, **kwargs):
    logger.debug("Consumer: %s" % (msg, ), *args, **kwargs)


class Component(StartStopComponent):
    name = "worker.consumer"
    last = True

    def Consumer(self, w):
        return (w.consumer_cls or
                Consumer if w.hub else BlockingConsumer)

    def create(self, w):
        prefetch_count = w.concurrency * w.prefetch_multiplier
        c = w.consumer = self.instantiate(self.Consumer(w),
                w.ready_queue,
                hostname=w.hostname,
                send_events=w.send_events,
                init_callback=w.ready_callback,
                initial_prefetch_count=prefetch_count,
                pool=w.pool,
                timer=w.timer,
                app=w.app,
                controller=w,
                hub=w.hub)
        return c


class QoS(object):
    """Quality of Service for Channel.

    For thread-safe increment/decrement of a channels prefetch count value.

    :param consumer: A :class:`kombu.messaging.Consumer` instance.
    :param initial_value: Initial prefetch count value.

    """
    prev = None

    def __init__(self, consumer, initial_value):
        self.consumer = consumer
        self._mutex = threading.RLock()
        self.value = initial_value

    def increment(self, n=1):
        """Increment the current prefetch count value by n."""
        with self._mutex:
            if self.value:
                new_value = self.value + max(n, 0)
                self.value = self.set(new_value)
        return self.value

    def _sub(self, n=1):
        assert self.value - n > 1
        self.value -= n

    def decrement(self, n=1):
        """Decrement the current prefetch count value by n."""
        with self._mutex:
            if self.value:
                self._sub(n)
                self.set(self.value)
        return self.value

    def decrement_eventually(self, n=1):
        """Decrement the value, but do not update the qos.

        The MainThread will be responsible for calling :meth:`update`
        when necessary.

        """
        with self._mutex:
            if self.value:
                self._sub(n)

    def set(self, pcount):
        """Set channel prefetch_count setting."""
        if pcount != self.prev:
            new_value = pcount
            if pcount > PREFETCH_COUNT_MAX:
                warn("QoS: Disabled: prefetch_count exceeds %r",
                     PREFETCH_COUNT_MAX)
                new_value = 0
            debug("basic.qos: prefetch_count->%s", new_value)
            self.consumer.qos(prefetch_count=new_value)
            self.prev = pcount
        return pcount

    def update(self):
        """Update prefetch count with current value."""
        with self._mutex:
            return self.set(self.value)


class Consumer(object):
    """Listen for messages received from the broker and
    move them to the ready queue for task processing.

    :param ready_queue: See :attr:`ready_queue`.
    :param timer: See :attr:`timer`.

    """

    #: The queue that holds tasks ready for immediate processing.
    ready_queue = None

    #: Enable/disable events.
    send_events = False

    #: Optional callback to be called when the connection is established.
    #: Will only be called once, even if the connection is lost and
    #: re-established.
    init_callback = None

    #: The current hostname.  Defaults to the system hostname.
    hostname = None

    #: Initial QoS prefetch count for the task channel.
    initial_prefetch_count = 0

    #: A :class:`celery.events.EventDispatcher` for sending events.
    event_dispatcher = None

    #: The thread that sends event heartbeats at regular intervals.
    #: The heartbeats are used by monitors to detect that a worker
    #: went offline/disappeared.
    heart = None

    #: The broker connection.
    connection = None

    #: The consumer used to consume task messages.
    task_consumer = None

    #: The consumer used to consume broadcast commands.
    broadcast_consumer = None

    #: The process mailbox (kombu pidbox node).
    pidbox_node = None
    _pidbox_node_shutdown = None   # used for greenlets
    _pidbox_node_stopped = None    # used for greenlets

    #: The current worker pool instance.
    pool = None

    #: A timer used for high-priority internal tasks, such
    #: as sending heartbeats.
    timer = None

    # Consumer state, can be RUN or CLOSE.
    _state = None

    def __init__(self, ready_queue,
            init_callback=noop, send_events=False, hostname=None,
            initial_prefetch_count=2, pool=None, app=None,
            timer=None, controller=None, hub=None, **kwargs):
        self.app = app_or_default(app)
        self.connection = None
        self.task_consumer = None
        self.controller = controller
        self.broadcast_consumer = None
        self.ready_queue = ready_queue
        self.send_events = send_events
        self.init_callback = init_callback
        self.hostname = hostname or socket.gethostname()
        self.initial_prefetch_count = initial_prefetch_count
        self.event_dispatcher = None
        self.heart = None
        self.pool = pool
        self.timer = timer or timer2.default_timer
        pidbox_state = AttributeDict(app=self.app,
                                     hostname=self.hostname,
                                     listener=self,     # pre 2.2
                                     consumer=self)
        self.pidbox_node = self.app.control.mailbox.Node(self.hostname,
                                                         state=pidbox_state,
                                                         handlers=Panel.data)
        conninfo = self.app.broker_connection()
        self.connection_errors = conninfo.connection_errors
        self.channel_errors = conninfo.channel_errors

        self._does_info = logger.isEnabledFor(logging.INFO)
        self.strategies = {}
        if hub:
            hub.on_init.append(self.on_poll_init)
        self.hub = hub
        self._quick_put = self.ready_queue.put

    def update_strategies(self):
        S = self.strategies
        app = self.app
        loader = app.loader
        hostname = self.hostname
        for name, task in self.app.tasks.iteritems():
            S[name] = task.start_strategy(app, self)
            task.__tracer__ = build_tracer(name, task, loader, hostname)

    def start(self):
        """Start the consumer.

        Automatically survives intermittent connection failure,
        and will retry establishing the connection and restart
        consuming messages.

        """

        self.init_callback(self)

        while self._state != CLOSE:
            self.maybe_shutdown()
            try:
                self.reset_connection()
                self.consume_messages()
            except self.connection_errors + self.channel_errors:
                error(RETRY_CONNECTION, exc_info=True)

    def on_poll_init(self, hub):
        hub.update_readers(self.connection.eventmap)
        self.connection.transport.on_poll_init(hub.poller)

    def consume_messages(self, sleep=sleep, min=min, Empty=Empty):
        """Consume messages forever (or until an exception is raised)."""

        with self.hub as hub:
            qos = self.qos
            update_qos = qos.update
            update_readers = hub.update_readers
            readers, writers = hub.readers, hub.writers
            poll = hub.poller.poll
            fire_timers = hub.fire_timers
            scheduled = hub.timer._queue
            on_poll_start = self.connection.transport.on_poll_start
            strategies = self.strategies
            connection = self.connection
            drain_nowait = connection.drain_nowait
            on_task_callbacks = hub.on_task
            buffer = []

            def flush_buffer():
                for name, body, message in buffer:
                    try:
                        strategies[name](message, body, message.ack_log_error)
                    except KeyError, exc:
                        self.handle_unknown_task(body, message, exc)
                    except InvalidTaskError, exc:
                        self.handle_invalid_task(body, message, exc)
                buffer[:] = []

            def on_task_received(body, message):
                if on_task_callbacks:
                    [callback() for callback in on_task_callbacks]
                try:
                    name = body["task"]
                except (KeyError, TypeError):
                    return self.handle_unknown_message(body, message)
                try:
                    strategies[name](message, body, message.ack_log_error)
                except KeyError, exc:
                    self.handle_unknown_task(body, message, exc)
                except InvalidTaskError, exc:
                    self.handle_invalid_task(body, message, exc)
                #bufferlen = len(buffer)
                #buffer.append((name, body, message))
                #if bufferlen + 1 >= 4:
                #    flush_buffer()
                #if bufferlen:
                #    fire_timers()

            self.task_consumer.callbacks = [on_task_received]
            self.task_consumer.consume()

            debug("Ready to accept tasks!")

            while self._state != CLOSE and self.connection:
                # shutdown if signal handlers told us to.
                if state.should_stop:
                    raise SystemExit()
                elif state.should_terminate:
                    raise SystemTerminate()

                # fire any ready timers, this also determines
                # when we need to wake up next.
                time_to_sleep = fire_timers() if scheduled else 1

                if qos.prev != qos.value:
                    update_qos()

                update_readers(on_poll_start())
                if readers or writers:
                    connection.more_to_read = True
                    while connection.more_to_read:
                        for fileno, event in poll(time_to_sleep) or ():
                            try:
                                if event & READ:
                                    readers[fileno](fileno, event)
                                if event & WRITE:
                                    writers[fileno](fileno, event)
                                if event & ERR:
                                    readers[fileno](fileno, event)
                                    writers[fileno](fileno, event)
                            except Empty:
                                break
                            except socket.error:
                                if self._state != CLOSE:  # pragma: no cover
                                    raise
                        if connection.more_to_read:
                            drain_nowait()
                            time_to_sleep = 0
                else:
                    sleep(min(time_to_sleep, 0.1))

    def on_task(self, task, task_reserved=task_reserved):
        """Handle received task.

        If the task has an `eta` we enter it into the ETA schedule,
        otherwise we move it the ready queue for immediate processing.

        """
        if task.revoked():
            return

        if self._does_info:
            info("Got task from broker: %s", task.shortinfo())

        if self.event_dispatcher.enabled:
            self.event_dispatcher.send("task-received", uuid=task.id,
                    name=task.name, args=safe_repr(task.args),
                    kwargs=safe_repr(task.kwargs),
                    retries=task.request_dict.get("retries", 0),
                    eta=task.eta and task.eta.isoformat(),
                    expires=task.expires and task.expires.isoformat())

        if task.eta:
            try:
                eta = timer2.to_timestamp(task.eta)
            except OverflowError, exc:
                error("Couldn't convert eta %s to timestamp: %r. Task: %r",
                      task.eta, exc, task.info(safe=True), exc_info=True)
                task.acknowledge()
            else:
                self.qos.increment()
                self.timer.apply_at(eta, self.apply_eta_task, (task, ),
                                    priority=6)
        else:
            task_reserved(task)
            self._quick_put(task)

    def on_control(self, body, message):
        """Process remote control command message."""
        try:
            self.pidbox_node.handle_message(body, message)
        except KeyError, exc:
            error("No such control command: %s", exc)
        except Exception, exc:
            error("Control command error: %r", exc, exc_info=True)
            self.reset_pidbox_node()

    def apply_eta_task(self, task):
        """Method called by the timer to apply a task with an
        ETA/countdown."""
        task_reserved(task)
        self._quick_put(task)
        self.qos.decrement_eventually()

    def _message_report(self, body, message):
        return MESSAGE_REPORT_FMT % (text.truncate(safe_repr(body), 1024),
                                     safe_repr(message.content_type),
                                     safe_repr(message.content_encoding),
                                     safe_repr(message.delivery_info))

    def handle_unknown_message(self, body, message):
        warn(UNKNOWN_FORMAT, self._message_report(body, message))
        message.reject_log_error(logger, self.connection_errors)

    def handle_unknown_task(self, body, message, exc):
        error(UNKNOWN_TASK_ERROR, exc, safe_repr(body), exc_info=True)
        message.reject_log_error(logger, self.connection_errors)

    def handle_invalid_task(self, body, message, exc):
        error(INVALID_TASK_ERROR, str(exc), safe_repr(body), exc_info=True)
        message.reject_log_error(logger, self.connection_errors)

    def receive_message(self, body, message):
        """Handles incoming messages.

        :param body: The message body.
        :param message: The kombu message object.

        """
        try:
            name = body["task"]
        except (KeyError, TypeError):
            return self.handle_unknown_message(body, message)

        try:
            self.strategies[name](message, body, message.ack_log_error)
        except KeyError, exc:
            self.handle_unknown_task(body, message, exc)
        except InvalidTaskError, exc:
            self.handle_invalid_task(body, message, exc)

    def maybe_conn_error(self, fun):
        """Applies function but ignores any connection or channel
        errors raised."""
        try:
            fun()
        except (AttributeError, ) + \
                self.connection_errors + \
                self.channel_errors:
            pass

    def close_connection(self):
        """Closes the current broker connection and all open channels."""

        # We must set self.connection to None here, so
        # that the green pidbox thread exits.
        connection, self.connection = self.connection, None

        if self.task_consumer:
            debug("Closing consumer channel...")
            self.task_consumer = \
                    self.maybe_conn_error(self.task_consumer.close)

        self.stop_pidbox_node()

        if connection:
            debug("Closing broker connection...")
            self.maybe_conn_error(connection.close)

    def stop_consumers(self, close_connection=True):
        """Stop consuming tasks and broadcast commands, also stops
        the heartbeat thread and event dispatcher.

        :keyword close_connection: Set to False to skip closing the broker
                                    connection.

        """
        if not self._state == RUN:
            return

        if self.heart:
            # Stop the heartbeat thread if it's running.
            debug("Heart: Going into cardiac arrest...")
            self.heart = self.heart.stop()

        debug("Cancelling task consumer...")
        if self.task_consumer:
            self.maybe_conn_error(self.task_consumer.cancel)

        if self.event_dispatcher:
            debug("Shutting down event dispatcher...")
            self.event_dispatcher = \
                    self.maybe_conn_error(self.event_dispatcher.close)

        debug("Cancelling broadcast consumer...")
        if self.broadcast_consumer:
            self.maybe_conn_error(self.broadcast_consumer.cancel)

        if close_connection:
            self.close_connection()

    def on_decode_error(self, message, exc):
        """Callback called if an error occurs while decoding
        a message received.

        Simply logs the error and acknowledges the message so it
        doesn't enter a loop.

        :param message: The message with errors.
        :param exc: The original exception instance.

        """
        crit("Can't decode message body: %r (type:%r encoding:%r raw:%r')",
             exc, message.content_type, message.content_encoding,
             text.truncate(safe_repr(message.body), 1024))
        message.ack()

    def reset_pidbox_node(self):
        """Sets up the process mailbox."""
        self.stop_pidbox_node()
        # close previously opened channel if any.
        if self.pidbox_node.channel:
            try:
                self.pidbox_node.channel.close()
            except self.connection_errors + self.channel_errors:
                pass

        if self.pool is not None and self.pool.is_green:
            return self.pool.spawn_n(self._green_pidbox_node)
        self.pidbox_node.channel = self.connection.channel()
        self.broadcast_consumer = self.pidbox_node.listen(
                                        callback=self.on_control)
        self.broadcast_consumer.consume()

    def stop_pidbox_node(self):
        if self._pidbox_node_stopped:
            self._pidbox_node_shutdown.set()
            debug("Waiting for broadcast thread to shutdown...")
            self._pidbox_node_stopped.wait()
            self._pidbox_node_stopped = self._pidbox_node_shutdown = None
        elif self.broadcast_consumer:
            debug("Closing broadcast channel...")
            self.broadcast_consumer = \
                self.maybe_conn_error(self.broadcast_consumer.channel.close)

    def _green_pidbox_node(self):
        """Sets up the process mailbox when running in a greenlet
        environment."""
        # THIS CODE IS TERRIBLE
        # Luckily work has already started rewriting the Consumer for 3.0.
        self._pidbox_node_shutdown = threading.Event()
        self._pidbox_node_stopped = threading.Event()
        try:
            with self._open_connection() as conn:
                self.pidbox_node.channel = conn.default_channel
                self.broadcast_consumer = self.pidbox_node.listen(
                                            callback=self.on_control)
                with self.broadcast_consumer:
                    while not self._pidbox_node_shutdown.isSet():
                        try:
                            conn.drain_events(timeout=1.0)
                        except socket.timeout:
                            pass
        finally:
            self._pidbox_node_stopped.set()

    def reset_connection(self):
        """Re-establish the broker connection and set up consumers,
        heartbeat and the event dispatcher."""
        debug("Re-establishing connection to the broker...")
        self.stop_consumers()

        # Clear internal queues to get rid of old messages.
        # They can't be acked anyway, as a delivery tag is specific
        # to the current channel.
        self.ready_queue.clear()
        self.timer.clear()

        # Re-establish the broker connection and setup the task consumer.
        self.connection = self._open_connection()
        debug("Connection established.")
        self.task_consumer = self.app.amqp.TaskConsumer(self.connection,
                                    on_decode_error=self.on_decode_error)
        # QoS: Reset prefetch window.
        self.qos = QoS(self.task_consumer, self.initial_prefetch_count)
        self.qos.update()

        # Setup the process mailbox.
        self.reset_pidbox_node()

        # Flush events sent while connection was down.
        prev_event_dispatcher = self.event_dispatcher
        self.event_dispatcher = self.app.events.Dispatcher(self.connection,
                                                hostname=self.hostname,
                                                enabled=self.send_events)
        if prev_event_dispatcher:
            self.event_dispatcher.copy_buffer(prev_event_dispatcher)
            self.event_dispatcher.flush()

        # Restart heartbeat thread.
        self.restart_heartbeat()

        # reload all task's execution strategies.
        self.update_strategies()

        # We're back!
        self._state = RUN

    def restart_heartbeat(self):
        """Restart the heartbeat thread.

        This thread sends heartbeat events at intervals so monitors
        can tell if the worker is off-line/missing.

        """
        self.heart = Heart(self.timer, self.event_dispatcher)
        self.heart.start()

    def _open_connection(self):
        """Establish the broker connection.

        Will retry establishing the connection if the
        :setting:`BROKER_CONNECTION_RETRY` setting is enabled

        """

        # Callback called for each retry while the connection
        # can't be established.
        def _error_handler(exc, interval):
            error("Consumer: Connection Error: %s. "
                  "Trying again in %d seconds...", exc, interval)

        # remember that the connection is lazy, it won't establish
        # until it's needed.
        conn = self.app.broker_connection()
        if not self.app.conf.BROKER_CONNECTION_RETRY:
            # retry disabled, just call connect directly.
            conn.connect()
            return conn

        return conn.ensure_connection(_error_handler,
                    self.app.conf.BROKER_CONNECTION_MAX_RETRIES,
                    callback=self.maybe_shutdown)

    def stop(self):
        """Stop consuming.

        Does not close the broker connection, so be sure to call
        :meth:`close_connection` when you are finished with it.

        """
        # Notifies other threads that this instance can't be used
        # anymore.
        self.close()
        debug("Stopping consumers...")
        self.stop_consumers(close_connection=False)

    def close(self):
        self._state = CLOSE

    def maybe_shutdown(self):
        if state.should_stop:
            raise SystemExit()
        elif state.should_terminate:
            raise SystemTerminate()

    @property
    def info(self):
        """Returns information about this consumer instance
        as a dict.

        This is also the consumer related info returned by
        ``celeryctl stats``.

        """
        conninfo = {}
        if self.connection:
            conninfo = self.connection.info()
            conninfo.pop("password", None)  # don't send password.
        return {"broker": conninfo, "prefetch_count": self.qos.value}


class BlockingConsumer(Consumer):

    def consume_messages(self):
        # receive_message handles incoming messages.
        self.task_consumer.register_callback(self.receive_message)
        self.task_consumer.consume()

        debug("Ready to accept tasks!")

        while self._state != CLOSE and self.connection:
            self.maybe_shutdown()
            if self.qos.prev != self.qos.value:     # pragma: no cover
                self.qos.update()
            try:
                self.connection.drain_events(timeout=10.0)
            except socket.timeout:
                pass
            except socket.error:
                if self._state != CLOSE:            # pragma: no cover
                    raise
