# -*- coding: utf-8 -*-
"""
celery.worker.consumer
~~~~~~~~~~~~~~~~~~~~~~

This module contains the components responsible for consuming messages
from the broker, processing the messages and keeping the broker connections
up and running.

"""
from __future__ import absolute_import

import kombu
import logging
import socket

from collections import defaultdict
from functools import partial
from heapq import heappush
from operator import itemgetter
from time import sleep

from billiard.common import restart_state
from billiard.exceptions import RestartFreqExceeded
from kombu.common import QoS, ignore_errors
from kombu.syn import _detect_environment
from kombu.utils.encoding import safe_repr

from celery import bootsteps
from celery.app import app_or_default
from celery.canvas import subtask
from celery.five import items, values
from celery.task.trace import build_tracer
from celery.utils.functional import noop
from celery.utils.log import get_logger
from celery.utils.text import truncate
from celery.utils.timer2 import default_timer, to_timestamp
from celery.utils.timeutils import humanize_seconds, timezone

from . import heartbeat, loops, pidbox
from .state import task_reserved, maybe_shutdown, revoked

CLOSE = bootsteps.CLOSE
logger = get_logger(__name__)
debug, info, warn, error, crit = (logger.debug, logger.info, logger.warn,
                                  logger.error, logger.critical)

CONNECTION_RETRY = """\
consumer: Connection to broker lost. \
Trying to re-establish the connection...\
"""

CONNECTION_RETRY_STEP = """\
Trying again {when}...\
"""

CONNECTION_ERROR = """\
consumer: Cannot connect to %s: %s.
%s
"""

CONNECTION_FAILOVER = """\
Will retry using next failover.\
"""

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

MESSAGE_REPORT = """\
body: {0} {{content_type:{1} content_encoding:{2} delivery_info:{3}}}\
"""


def dump_body(m, body):
    return '{0} ({1}b)'.format(truncate(safe_repr(body), 1024),
                               len(m.body))


class Consumer(object):

    #: Intra-queue for tasks ready to be handled
    ready_queue = None

    #: Optional callback called the first time the worker
    #: is ready to receive tasks.
    init_callback = None

    #: The current worker pool instance.
    pool = None

    #: A timer used for high-priority internal tasks, such
    #: as sending heartbeats.
    timer = None

    restart_count = -1  # first start is the same as a restart

    class Namespace(bootsteps.Namespace):
        name = 'Consumer'
        default_steps = [
            'celery.worker.consumer:Connection',
            'celery.worker.consumer:Mingle',
            'celery.worker.consumer:Events',
            'celery.worker.consumer:Gossip',
            'celery.worker.consumer:Heart',
            'celery.worker.consumer:Control',
            'celery.worker.consumer:Tasks',
            'celery.worker.consumer:Evloop',
            'celery.worker.consumer:Agent',
        ]

        def shutdown(self, parent):
            self.restart(parent, 'Shutdown', 'shutdown')

    def __init__(self, ready_queue,
                 init_callback=noop, hostname=None,
                 pool=None, app=None,
                 timer=None, controller=None, hub=None, amqheartbeat=None,
                 worker_options=None, **kwargs):
        self.app = app_or_default(app)
        self.controller = controller
        self.ready_queue = ready_queue
        self.init_callback = init_callback
        self.hostname = hostname or socket.gethostname()
        self.pool = pool
        self.timer = timer or default_timer
        self.strategies = {}
        conninfo = self.app.connection()
        self.connection_errors = conninfo.connection_errors
        self.channel_errors = conninfo.channel_errors
        self._restart_state = restart_state(maxR=5, maxT=1)

        self._does_info = logger.isEnabledFor(logging.INFO)
        self._quick_put = self.ready_queue.put
        self.amqheartbeat_rate = self.app.conf.BROKER_HEARTBEAT_CHECKRATE

        if hub:
            self.amqheartbeat = amqheartbeat
            if self.amqheartbeat is None:
                self.amqheartbeat = self.app.conf.BROKER_HEARTBEAT
            self.hub = hub
            self.hub.on_init.append(self.on_poll_init)
        else:
            self.hub = None
            self.amqheartbeat = 0

        if not hasattr(self, 'loop'):
            self.loop = loops.asynloop if hub else loops.synloop

        if _detect_environment() == 'gevent':
            # there's a gevent bug that causes timeouts to not be reset,
            # so if the connection timeout is exceeded once, it can NEVER
            # connect again.
            self.app.conf.BROKER_CONNECTION_TIMEOUT = None

        self.steps = []
        self.namespace = self.Namespace(
            app=self.app, on_close=self.on_close,
        )
        self.namespace.apply(self, **dict(worker_options or {}, **kwargs))

    def start(self):
        ns, loop = self.namespace, self.loop
        while ns.state != CLOSE:
            self.restart_count += 1
            maybe_shutdown()
            try:
                ns.start(self)
            except self.connection_errors:
                maybe_shutdown()
                try:
                    self._restart_state.step()
                except RestartFreqExceeded as exc:
                    crit('Frequent restarts detected: %r', exc, exc_info=1)
                    sleep(1)
                if ns.state != CLOSE and self.connection:
                    warn(CONNECTION_RETRY, exc_info=True)
                    ns.restart(self)

    def shutdown(self):
        self.namespace.shutdown(self)

    def stop(self):
        self.namespace.stop(self)

    def on_ready(self):
        callback, self.init_callback = self.init_callback, None
        if callback:
            callback(self)

    def loop_args(self):
        return (self, self.connection, self.task_consumer,
                self.strategies, self.namespace, self.hub, self.qos,
                self.amqheartbeat, self.handle_unknown_message,
                self.handle_unknown_task, self.handle_invalid_task,
                self.app.clock, self.amqheartbeat_rate)

    def on_poll_init(self, hub):
        hub.update_readers(self.connection.eventmap)
        self.connection.transport.on_poll_init(hub.poller)

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
             dump_body(message, message.body))
        message.ack()

    def on_close(self):
        # Clear internal queues to get rid of old messages.
        # They can't be acked anyway, as a delivery tag is specific
        # to the current channel.
        self.ready_queue.clear()
        self.timer.clear()

    def connect(self):
        """Establish the broker connection.

        Will retry establishing the connection if the
        :setting:`BROKER_CONNECTION_RETRY` setting is enabled

        """
        conn = self.app.connection(heartbeat=self.amqheartbeat)

        # Callback called for each retry while the connection
        # can't be established.
        def _error_handler(exc, interval, next_step=CONNECTION_RETRY_STEP):
            if getattr(conn, 'alt', None) and interval == 0:
                next_step = CONNECTION_FAILOVER
            error(CONNECTION_ERROR, conn.as_uri(), exc,
                  next_step.format(when=humanize_seconds(interval, 'in', ' ')))

        # remember that the connection is lazy, it won't establish
        # until it's needed.
        if not self.app.conf.BROKER_CONNECTION_RETRY:
            # retry disabled, just call connect directly.
            conn.connect()
            return conn

        return conn.ensure_connection(
            _error_handler, self.app.conf.BROKER_CONNECTION_MAX_RETRIES,
            callback=maybe_shutdown,
        )

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
            info('Started consuming from %r', queue)

    def cancel_task_queue(self, queue):
        self.app.amqp.queues.select_remove(queue)
        self.task_consumer.cancel_by_queue(queue)

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
            except OverflowError as exc:
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

    def apply_eta_task(self, task):
        """Method called by the timer to apply a task with an
        ETA/countdown."""
        task_reserved(task)
        self._quick_put(task)
        self.qos.decrement_eventually()

    def _message_report(self, body, message):
        return MESSAGE_REPORT.format(dump_body(message, body),
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

    def update_strategies(self):
        loader = self.app.loader
        for name, task in items(self.app.tasks):
            self.strategies[name] = task.start_strategy(self.app, self)
            task.__trace__ = build_tracer(name, task, loader, self.hostname)


class Connection(bootsteps.StartStopStep):

    def __init__(self, c, **kwargs):
        c.connection = None

    def start(self, c):
        c.connection = c.connect()
        info('Connected to %s', c.connection.as_uri())

    def shutdown(self, c):
        # We must set self.connection to None here, so
        # that the green pidbox thread exits.
        connection, c.connection = c.connection, None
        if connection:
            ignore_errors(connection, connection.close)

    def info(self, c):
        info = c.connection.info()
        info.pop('password', None)  # don't send password.
        return {'broker': info}


class Events(bootsteps.StartStopStep):
    requires = (Connection, )

    def __init__(self, c, send_events=None, **kwargs):
        self.send_events = True
        self.groups = None if send_events else ['worker']
        c.event_dispatcher = None

    def start(self, c):
        # Flush events sent while connection was down.
        prev = c.event_dispatcher
        dis = c.event_dispatcher = c.app.events.Dispatcher(
            c.connection, hostname=c.hostname,
            enabled=self.send_events, groups=self.groups,
        )
        if prev:
            dis.copy_buffer(prev)
            dis.flush()

    def stop(self, c):
        if c.event_dispatcher:
            ignore_errors(c, c.event_dispatcher.close)
            c.event_dispatcher = None
    shutdown = stop


class Heart(bootsteps.StartStopStep):
    requires = (Events, )

    def __init__(self, c, enable_heartbeat=True, **kwargs):
        self.enabled = enable_heartbeat
        c.heart = None

    def start(self, c):
        c.heart = heartbeat.Heart(c.timer, c.event_dispatcher)
        c.heart.start()

    def stop(self, c):
        c.heart = c.heart and c.heart.stop()
    shutdown = stop


class Control(bootsteps.StartStopStep):
    requires = (Events, )

    def __init__(self, c, **kwargs):
        self.is_green = c.pool is not None and c.pool.is_green
        self.box = (pidbox.gPidbox if self.is_green else pidbox.Pidbox)(c)
        self.start = self.box.start
        self.stop = self.box.stop
        self.shutdown = self.box.shutdown


class Tasks(bootsteps.StartStopStep):
    requires = (Control, )

    def __init__(self, c, initial_prefetch_count=2, **kwargs):
        c.task_consumer = c.qos = None
        self.initial_prefetch_count = initial_prefetch_count

    def start(self, c):
        c.update_strategies()
        c.task_consumer = c.app.amqp.TaskConsumer(
            c.connection, on_decode_error=c.on_decode_error,
        )
        c.qos = QoS(c.task_consumer.qos, self.initial_prefetch_count)
        c.qos.update()  # set initial prefetch count

    def stop(self, c):
        if c.task_consumer:
            debug('Cancelling task consumer...')
            ignore_errors(c, c.task_consumer.cancel)

    def shutdown(self, c):
        if c.task_consumer:
            self.stop(c)
            debug('Closing consumer channel...')
            ignore_errors(c, c.task_consumer.close)
            c.task_consumer = None

    def info(self, c):
        return {'prefetch_count': c.qos.value}


class Agent(bootsteps.StartStopStep):
    conditional = True
    requires = (Connection, )

    def __init__(self, c, **kwargs):
        self.agent_cls = self.enabled = c.app.conf.CELERYD_AGENT

    def create(self, c):
        agent = c.agent = self.instantiate(self.agent_cls, c.connection)
        return agent


class Mingle(bootsteps.StartStopStep):
    label = 'Mingle'
    requires = (Connection, )

    def __init__(self, c, enable_mingle=True, **kwargs):
        self.enabled = enable_mingle

    def start(self, c):
        info('mingle: searching for neighbors')
        I = c.app.control.inspect(timeout=1.0, connection=c.connection)
        replies = I.hello()
        if replies:
            for reply in values(replies):
                c.app.clock.adjust(reply['clock'])
                revoked.update(reply['revoked'])
            info('mingle: synced with %s', ', '.join(replies))
        else:
            info('mingle: no one here')


class Gossip(bootsteps.ConsumerStep):
    label = 'Gossip'
    requires = (Events, )
    _cons_stamp_fields = itemgetter(
        'clock', 'hostname', 'pid', 'topic', 'action',
    )

    def __init__(self, c, enable_gossip=True, interval=5.0, **kwargs):
        self.enabled = enable_gossip
        self.app = c.app
        c.gossip = self
        self.Receiver = c.app.events.Receiver
        self.hostname = c.hostname

        self.timer = c.timer
        self.state = c.app.events.State()
        self.interval = interval
        self._tref = None
        self.consensus_requests = defaultdict(list)
        self.consensus_replies = {}
        self.update_state = self.state.worker_event
        self.event_handlers = {
            'worker.elect': self.on_elect,
            'worker.elect.ack': self.on_elect_ack,
        }
        self.clock = c.app.clock

        self.election_handlers = {
            'task': self.call_task
        }

    def election(self, id, topic, action=None):
        self.consensus_replies[id] = []
        self.dispatcher.send('worker-elect', id=id, topic=topic, action=action)

    def call_task(self, task):
        try:
            X = subtask(task)
            X.apply_async()
        except Exception as exc:
            error('Could not call task: %r', exc, exc_info=1)

    def on_elect(self, event):
        id = event['id']
        self.dispatcher.send('worker-elect-ack', id=id)
        clock, hostname, pid, topic, action = self._cons_stamp_fields(event)
        heappush(
            self.consensus_requests[id],
            (clock, '%s.%s' % (hostname, pid), topic, action),
        )

    def start(self, c):
        super(Gossip, self).start(c)
        self.dispatcher = c.event_dispatcher

    def on_elect_ack(self, event):
        id = event['id']
        try:
            replies = self.consensus_replies[id]
        except KeyError:
            return
        alive_workers = self.state.alive_workers()
        replies.append(event['hostname'])

        if len(replies) >= len(alive_workers):
            _, leader, topic, action = self.lock.sort_heap(
                self.consensus_requests[id],
            )
            if leader == self.hostname:
                info('I won the election %r', id)
                try:
                    handler = self.election_handlers[topic]
                except KeyError:
                    error('Unknown election topic %r', topic, exc_info=1)
                else:
                    handler(action)
            else:
                info('node %s elected for %r', leader, id)
            self.consensus_requests.pop(id, None)
            self.consensus_replies.pop(id, None)

    def on_node_join(self, worker):
        info('%s joined the party', worker.hostname)

    def on_node_leave(self, worker):
        info('%s left', worker.hostname)

    def on_node_lost(self, worker):
        warn('%s went missing!', worker.hostname)

    def register_timer(self):
        if self._tref is not None:
            self._tref.cancel()
        self.timer.apply_interval(self.interval * 1000.0, self.periodic)

    def periodic(self):
        for worker in values(self.state.workers):
            if not worker.alive:
                try:
                    self.on_node_lost(worker)
                finally:
                    self.state.workers.pop(worker.hostname, None)

    def get_consumers(self, channel):
        self.register_timer()
        ev = self.Receiver(channel, routing_key='worker.#')
        return [kombu.Consumer(
            channel,
            queues=[ev.queue],
            on_message=partial(self.on_message, ev.event_from_message),
            no_ack=True
        )]

    def on_message(self, prepare, message):
        _type = message.delivery_info['routing_key']
        try:
            handler = self.event_handlers[_type]
        except KeyError:
            pass
        else:
            return handler(message.payload)

        hostname = (message.headers.get('hostname') or
                    message.payload['hostname'])
        if hostname != self.hostname:
            type, event = prepare(message.payload)
            group, _, subject = type.partition('-')
            worker, created = self.update_state(subject, event)
            if subject == 'offline':
                try:
                    self.on_node_leave(worker)
                finally:
                    self.state.workers.pop(worker.hostname, None)
            elif created or subject == 'online':
                self.on_node_join(worker)
        else:
            self.clock.forward()


class Evloop(bootsteps.StartStopStep):
    label = 'event loop'
    last = True

    def start(self, c):
        c.loop(*c.loop_args())
