# -*- coding: utf-8 -*-
"""
celery.worker.consumer
~~~~~~~~~~~~~~~~~~~~~~

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
  immediately because most broker clients don't support doing broker
  requests across threads.  Instead the current prefetch count is kept as a
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

from kombu.syn import _detect_environment
from kombu.utils.encoding import safe_repr, safe_str, bytes_t
from kombu.utils.eventio import READ, WRITE, ERR

from celery.app import app_or_default
from celery.datastructures import AttributeDict
from celery.exceptions import InvalidTaskError, SystemTerminate
from celery.task.trace import build_tracer
from celery.utils import text
from celery.utils import timer2
from celery.utils.functional import noop
from celery.utils.log import get_logger
from celery.utils.timer2 import to_timestamp
from celery.utils.timeutils import humanize_seconds, timezone

from . import state
from .bootsteps import StartStopComponent
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
More: http://docs.celeryq.org/en/latest/userguide/tasks.html#names

The full contents of the message body was:
%s
"""

#: Error message for when an invalid task message is received.
INVALID_TASK_ERROR = """\
Received invalid task message: %s
The message has been ignored and discarded.

Please ensure your message conforms to the task message format:
http://docs.celeryq.org/en/latest/internals/protocol.html

The full contents of the message body was:
%s
"""

MESSAGE_REPORT = """\
body: %s {content_type:%s content_encoding:%s delivery_info:%s}\
"""


RETRY_CONNECTION = """\
consumer: Connection to broker lost. \
Trying to re-establish the connection...\
"""

CONNECTION_ERROR = """\
consumer: Cannot connect to %s: %s.
%s
"""

CONNECTION_RETRY = """\
Trying again %(when)s...\
"""

CONNECTION_FAILOVER = """\
Will retry using next failover.\
"""

task_reserved = state.task_reserved

logger = get_logger(__name__)
info, warn, error, crit = (logger.info, logger.warning,
                           logger.error, logger.critical)


def debug(msg, *args, **kwargs):
    logger.debug('consumer: %s' % (msg, ), *args, **kwargs)


def dump_body(m, body):
    if isinstance(body, buffer):
        body = bytes_t(body)
    return "%s (%sb)" % (text.truncate(safe_repr(body), 1024), len(m.body))


class Component(StartStopComponent):
    name = 'worker.consumer'
    last = True

    def Consumer(self, w):
        return (w.consumer_cls or
                Consumer if w.hub else BlockingConsumer)

    def create(self, w):
        prefetch_count = w.concurrency * w.prefetch_multiplier
        c = w.consumer = self.instantiate(
            self.Consumer(w),
            w.ready_queue,
            hostname=w.hostname,
            send_events=w.send_events,
            init_callback=w.ready_callback,
            initial_prefetch_count=prefetch_count,
            pool=w.pool,
            timer=w.timer,
            app=w.app,
            controller=w,
            hub=w.hub,
        )
        return c


class QoS(object):
    """Thread safe increment/decrement of a channels prefetch_count.

    :param consumer: A :class:`kombu.messaging.Consumer` instance.
    :param initial_value: Initial prefetch count value.

    """
    prev = None

    def __init__(self, consumer, initial_value):
        self.consumer = consumer
        self._mutex = threading.RLock()
        self.value = initial_value or 0

    def increment_eventually(self, n=1):
        """Increment the value, but do not update the channels QoS.

        The MainThread will be responsible for calling :meth:`update`
        when necessary.

        """
        with self._mutex:
            if self.value:
                self.value = self.value + max(n, 0)
        return self.value

    def decrement_eventually(self, n=1):
        """Decrement the value, but do not update the channels QoS.

        The MainThread will be responsible for calling :meth:`update`
        when necessary.

        """
        with self._mutex:
            if self.value:
                self.value -= n
        return self.value

    def set(self, pcount):
        """Set channel prefetch_count setting."""
        if pcount != self.prev:
            new_value = pcount
            if pcount > PREFETCH_COUNT_MAX:
                warn('QoS: Disabled: prefetch_count exceeds %r',
                     PREFETCH_COUNT_MAX)
                new_value = 0
            debug('basic.qos: prefetch_count->%s', new_value)
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

    restart_count = -1  # first start is the same as a restart

    def __init__(self, ready_queue,
                 init_callback=noop, send_events=False, hostname=None,
                 initial_prefetch_count=2, pool=None, app=None,
                 timer=None, controller=None, hub=None, amqheartbeat=None,
                 **kwargs):
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
        self.pidbox_node = self.app.control.mailbox.Node(
            safe_str(self.hostname), state=pidbox_state, handlers=Panel.data,
        )
        conninfo = self.app.connection()
        self.connection_errors = conninfo.connection_errors
        self.channel_errors = conninfo.channel_errors

        self._does_info = logger.isEnabledFor(logging.INFO)
        self.strategies = {}
        if hub:
            hub.on_init.append(self.on_poll_init)
        self.hub = hub
        self._quick_put = self.ready_queue.put
        self.amqheartbeat = amqheartbeat
        if self.amqheartbeat is None:
            self.amqheartbeat = self.app.conf.BROKER_HEARTBEAT
        if not hub:
            self.amqheartbeat = 0

        if _detect_environment() == 'gevent':
            # there's a gevent bug that causes timeouts to not be reset,
            # so if the connection timeout is exceeded once, it can NEVER
            # connect again.
            self.app.conf.BROKER_CONNECTION_TIMEOUT = None

    def update_strategies(self):
        S = self.strategies
        app = self.app
        loader = app.loader
        hostname = self.hostname
        for name, task in self.app.tasks.iteritems():
            S[name] = task.start_strategy(app, self)
            task.__trace__ = build_tracer(name, task, loader, hostname)

    def start(self):
        """Start the consumer.

        Automatically survives intermittent connection failure,
        and will retry establishing the connection and restart
        consuming messages.

        """

        self.init_callback(self)

        while self._state != CLOSE:
            self.restart_count += 1
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
        hbrate = self.app.conf.BROKER_HEARTBEAT_CHECKRATE

        with self.hub as hub:
            qos = self.qos
            update_qos = qos.update
            update_readers = hub.update_readers
            readers, writers = hub.readers, hub.writers
            poll = hub.poller.poll
            fire_timers = hub.fire_timers
            scheduled = hub.timer._queue
            connection = self.connection
            hb = self.amqheartbeat
            hbtick = connection.heartbeat_check
            on_poll_start = connection.transport.on_poll_start
            on_poll_empty = connection.transport.on_poll_empty
            strategies = self.strategies
            drain_nowait = connection.drain_nowait
            on_task_callbacks = hub.on_task
            keep_draining = connection.transport.nb_keep_draining
            errors = connection.connection_errors

            if hb and connection.supports_heartbeats:
                hub.timer.apply_interval(
                    hb * 1000.0 / hbrate, hbtick, (hbrate, ))

            def on_task_received(body, message):
                if on_task_callbacks:
                    [callback() for callback in on_task_callbacks]
                try:
                    name = body['task']
                except (KeyError, TypeError):
                    return self.handle_unknown_message(body, message)
                try:
                    strategies[name](message, body, message.ack_log_error)
                except KeyError, exc:
                    self.handle_unknown_task(body, message, exc)
                except InvalidTaskError, exc:
                    self.handle_invalid_task(body, message, exc)

            self.task_consumer.callbacks = [on_task_received]
            self.task_consumer.consume()

            debug('Ready to accept tasks!')

            while self._state != CLOSE and self.connection:
                # shutdown if signal handlers told us to.
                if state.should_stop:
                    raise SystemExit()
                elif state.should_terminate:
                    raise SystemTerminate()

                # fire any ready timers, this also returns
                # the number of seconds until we need to fire timers again.
                poll_timeout = (fire_timers(propagate=errors) if scheduled
                                else 1)

                # We only update QoS when there is no more messages to read.
                # This groups together qos calls, and makes sure that remote
                # control commands will be prioritized over task messages.
                if qos.prev != qos.value:
                    update_qos()

                update_readers(on_poll_start())
                if readers or writers:
                    connection.more_to_read = True
                    while connection.more_to_read:
                        try:
                            events = poll(poll_timeout)
                        except ValueError:  # Issue 882
                            return
                        if not events:
                            on_poll_empty()
                        for fileno, event in events or ():
                            try:
                                if event & READ:
                                    readers[fileno](fileno, event)
                                if event & WRITE:
                                    writers[fileno](fileno, event)
                                if event & ERR:
                                    for handlermap in readers, writers:
                                        try:
                                            handlermap[fileno](fileno, event)
                                        except KeyError:
                                            pass
                            except (KeyError, Empty):
                                continue
                            except socket.error:
                                if self._state != CLOSE:  # pragma: no cover
                                    raise
                        if keep_draining:
                            drain_nowait()
                            poll_timeout = 0
                        else:
                            connection.more_to_read = False
                else:
                    # no sockets yet, startup is probably not done.
                    sleep(min(poll_timeout, 0.1))

    def on_task(self, task, task_reserved=task_reserved,
                to_system_tz=timezone.to_system):
        """Handle received task.

        If the task has an `eta` we enter it into the ETA schedule,
        otherwise we move it the ready queue for immediate processing.

        """
        if task.revoked():
            return

        if self._does_info:
            info('Got task from broker: %s', task)

        if self.event_dispatcher.enabled:
            self.event_dispatcher.send(
                'task-received',
                uuid=task.id, name=task.name,
                args=safe_repr(task.args), kwargs=safe_repr(task.kwargs),
                retries=task.request_dict.get('retries', 0),
                eta=task.eta and task.eta.isoformat(),
                expires=task.expires and task.expires.isoformat(),
            )

        if task.eta:
            try:
                if task.utc:
                    eta = to_timestamp(to_system_tz(task.eta))
                else:
                    eta = to_timestamp(task.eta, timezone.local)
            except OverflowError, exc:
                error("Couldn't convert eta %s to timestamp: %r. Task: %r",
                      task.eta, exc, task.info(safe=True), exc_info=True)
                task.acknowledge()
            else:
                self.qos.increment_eventually()
                self.timer.apply_at(
                    eta, self.apply_eta_task, (task, ), priority=6,
                )
        else:
            task_reserved(task)
            self._quick_put(task)

    def on_control(self, body, message):
        """Process remote control command message."""
        try:
            self.pidbox_node.handle_message(body, message)
        except KeyError, exc:
            error('No such control command: %s', exc)
        except Exception, exc:
            error('Control command error: %r', exc, exc_info=True)
            self.reset_pidbox_node()

    def apply_eta_task(self, task):
        """Method called by the timer to apply a task with an
        ETA/countdown."""
        task_reserved(task)
        self._quick_put(task)
        self.qos.decrement_eventually()

    def _message_report(self, body, message):
        return MESSAGE_REPORT % (dump_body(message, body),
                                 safe_repr(message.content_type),
                                 safe_repr(message.content_encoding),
                                 safe_repr(message.delivery_info))

    def handle_unknown_message(self, body, message):
        warn(UNKNOWN_FORMAT, self._message_report(body, message))
        message.reject_log_error(logger, self.connection_errors)

    def handle_unknown_task(self, body, message, exc):
        error(UNKNOWN_TASK_ERROR, exc, dump_body(message, body), exc_info=True)
        message.reject_log_error(logger, self.connection_errors)

    def handle_invalid_task(self, body, message, exc):
        error(INVALID_TASK_ERROR, exc, dump_body(message, body), exc_info=True)
        message.reject_log_error(logger, self.connection_errors)

    def receive_message(self, body, message):
        """Handles incoming messages.

        :param body: The message body.
        :param message: The kombu message object.

        """
        try:
            name = body['task']
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
            debug('Closing consumer channel...')
            self.task_consumer = self.maybe_conn_error(
                self.task_consumer.close)

        self.stop_pidbox_node()

        if connection:
            debug('Closing broker connection...')
            self.maybe_conn_error(connection.close)

    def stop_consumers(self, close_connection=True, join=True):
        """Stop consuming tasks and broadcast commands, also stops
        the heartbeat thread and event dispatcher.

        :keyword close_connection: Set to False to skip closing the broker
                                    connection.

        """
        if not self._state == RUN:
            return

        if self.heart:
            # Stop the heartbeat thread if it's running.
            debug('Heart: Going into cardiac arrest...')
            self.heart = self.heart.stop()

        debug('Cancelling task consumer...')
        if join and self.task_consumer:
            self.maybe_conn_error(self.task_consumer.cancel)

        if self.event_dispatcher:
            debug('Shutting down event dispatcher...')
            self.event_dispatcher = self.maybe_conn_error(
                self.event_dispatcher.close)

        debug('Cancelling broadcast consumer...')
        if join and self.broadcast_consumer:
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
             dump_body(message, message.body), exc_info=1)
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
            callback=self.on_control,
        )

    def stop_pidbox_node(self):
        if self._pidbox_node_stopped:
            self._pidbox_node_shutdown.set()
            debug('Waiting for broadcast thread to shutdown...')
            self._pidbox_node_stopped.wait()
            self._pidbox_node_stopped = self._pidbox_node_shutdown = None
        elif self.broadcast_consumer:
            debug('Closing broadcast channel...')
            self.broadcast_consumer = \
                self.maybe_conn_error(self.broadcast_consumer.channel.close)

    def _green_pidbox_node(self):
        """Sets up the process mailbox when running in a greenlet
        environment."""
        # THIS CODE IS TERRIBLE
        # Luckily work has already started rewriting the Consumer for 4.0.
        self._pidbox_node_shutdown = threading.Event()
        self._pidbox_node_stopped = threading.Event()
        try:
            with self._open_connection() as conn:
                info('pidbox: Connected to %s.', conn.as_uri())
                self.pidbox_node.channel = conn.default_channel
                self.broadcast_consumer = self.pidbox_node.listen(
                    callback=self.on_control,
                )

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
        debug('Re-establishing connection to the broker...')
        self.stop_consumers(join=False)

        # Clear internal queues to get rid of old messages.
        # They can't be acked anyway, as a delivery tag is specific
        # to the current channel.
        self.ready_queue.clear()
        self.timer.clear()

        # Re-establish the broker connection and setup the task consumer.
        self.connection = self._open_connection()
        info('consumer: Connected to %s.', self.connection.as_uri())
        self.task_consumer = self.app.amqp.TaskConsumer(
            self.connection, on_decode_error=self.on_decode_error,
        )
        # QoS: Reset prefetch window.
        self.qos = QoS(self.task_consumer, self.initial_prefetch_count)
        self.qos.update()

        # Setup the process mailbox.
        self.reset_pidbox_node()

        # Flush events sent while connection was down.
        prev_event_dispatcher = self.event_dispatcher
        self.event_dispatcher = self.app.events.Dispatcher(
            self.connection, hostname=self.hostname, enabled=self.send_events,
        )
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
        conn = self.app.connection(heartbeat=self.amqheartbeat)

        # Callback called for each retry while the connection
        # can't be established.
        def _error_handler(exc, interval, next_step=CONNECTION_RETRY):
            if getattr(conn, 'alt', None) and interval == 0:
                next_step = CONNECTION_FAILOVER
            error(CONNECTION_ERROR, conn.as_uri(), exc,
                  next_step % {'when': humanize_seconds(interval, 'in', ' ')})

        # remember that the connection is lazy, it won't establish
        # until it's needed.
        if not self.app.conf.BROKER_CONNECTION_RETRY:
            # retry disabled, just call connect directly.
            conn.connect()
            return conn

        return conn.ensure_connection(
            _error_handler, self.app.conf.BROKER_CONNECTION_MAX_RETRIES,
            callback=self.maybe_shutdown,
        )

    def stop(self):
        """Stop consuming.

        Does not close the broker connection, so be sure to call
        :meth:`close_connection` when you are finished with it.

        """
        # Notifies other threads that this instance can't be used
        # anymore.
        self.close()
        debug('Stopping consumers...')
        self.stop_consumers(close_connection=False, join=True)

    def close(self):
        self._state = CLOSE

    def maybe_shutdown(self):
        if state.should_stop:
            raise SystemExit()
        elif state.should_terminate:
            raise SystemTerminate()

    def add_task_queue(self, queue, exchange=None, exchange_type=None,
                       routing_key=None, **options):
        cset = self.task_consumer
        queues = self.app.amqp.queues
        # Must use in' here, as __missing__ will automatically
        # create queues when CELERY_CREATE_MISSING_QUEUES is enabled.
        # (Issue #1079)
        if queue in queues:
            q = queues[queue]
        else:
            exchange = queue if exchange is None else exchange
            exchange_type = ('direct' if exchange_type is None
                             else exchange_type)
            q = queues.select_add(queue,
                                  exchange=exchange,
                                  exchange_type=exchange_type,
                                  routing_key=routing_key, **options)
        if not cset.consuming_from(queue):
            cset.add_queue(q)
            cset.consume()
            logger.info('Started consuming from %r', queue)

    def cancel_task_queue(self, queue):
        self.app.amqp.queues.select_remove(queue)
        self.task_consumer.cancel_by_queue(queue)

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
            conninfo.pop('password', None)  # don't send password.
        return {'broker': conninfo, 'prefetch_count': self.qos.value}


class BlockingConsumer(Consumer):

    def consume_messages(self):
        # receive_message handles incoming messages.
        self.task_consumer.register_callback(self.receive_message)
        self.task_consumer.consume()

        debug('Ready to accept tasks!')

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
