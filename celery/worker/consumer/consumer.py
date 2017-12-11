# -*- coding: utf-8 -*-
"""Worker Consumer Blueprint.

This module contains the components responsible for consuming messages
from the broker, processing the messages and keeping the broker connections
up and running.
"""
import errno
import logging
import os
from collections import defaultdict
from time import sleep
from typing import Callable, Mapping, Tuple

from billiard.common import restart_state
from billiard.exceptions import RestartFreqExceeded
from kombu.async.semaphore import DummyLock
from kombu.types import ConnectionT, MessageT
from kombu.utils.compat import _detect_environment
<<<<<<< HEAD
from kombu.utils.encoding import bytes_t, safe_repr
=======
from kombu.utils.encoding import safe_repr
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from kombu.utils.limits import TokenBucket
from vine import Thenable, ppartial, promise

from celery import bootsteps, signals
from celery.app.trace import build_tracer
from celery.exceptions import InvalidTaskError, NotRegistered
from celery.types import (
    AppT, LoopT, PoolT, RequestT, TaskT, TimerT, WorkerT, WorkerConsumerT,
)
from celery.utils.functional import noop
from celery.utils.log import get_logger
from celery.utils.nodenames import gethostname
from celery.utils.objects import Bunch
from celery.utils.text import truncate
from celery.utils.time import humanize_seconds, rate
from celery.worker import loops
from celery.worker.state import (maybe_shutdown, reserved_requests,
                                 task_reserved)

__all__ = ('Consumer', 'Evloop', 'dump_body')

CLOSE = bootsteps.CLOSE
TERMINATE = bootsteps.TERMINATE
STOP_CONDITIONS = {CLOSE, TERMINATE}
logger = get_logger(__name__)
debug, info, warn, error, crit = (logger.debug, logger.info, logger.warning,
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
Received and deleted unknown message.  Wrong destination?!?

The full contents of the message body was: %s
"""

#: Error message for when an unregistered task is received.
UNKNOWN_TASK_ERROR = """\
Received unregistered task of type %s.
The message has been ignored and discarded.

Did you remember to import the module containing this task?
Or maybe you're using relative imports?

Please see
http://docs.celeryq.org/en/latest/internals/protocol.html
for more information.

The full contents of the message body was:
%s
"""

#: Error message for when an invalid task message is received.
INVALID_TASK_ERROR = """\
Received invalid task message: %s
The message has been ignored and discarded.

Please ensure your message conforms to the task
message protocol as described here:
http://docs.celeryq.org/en/latest/internals/protocol.html

The full contents of the message body was:
%s
"""

MESSAGE_DECODE_ERROR = """\
Can't decode message body: %r [type:%r encoding:%r headers:%s]

body: %s
"""

MESSAGE_REPORT = """\
body: {0}
{{content_type:{1} content_encoding:{2}
  delivery_info:{3} headers={4}}}
"""


def dump_body(m: MessageT, body: bytes):
    """Format message body for debugging purposes."""
    # v2 protocol does not deserialize body
    body = m.body if body is None else body
    return '{0} ({1}b)'.format(
        truncate(safe_repr(body), 1024), len(m.body))


class Consumer:
    """Consumer blueprint."""

    Strategies = dict

    #: Optional callback called the first time the worker
    #: is ready to receive tasks.
    init_callback = None

    #: The current worker pool instance.
    pool = None

    #: A timer used for high-priority internal tasks, such
    #: as sending heartbeats.
    timer = None

    restart_count = -1  # first start is the same as a restart

    class Blueprint(bootsteps.Blueprint):
        """Consumer blueprint."""

        name = 'Consumer'
        default_steps = [
            'celery.worker.consumer.connection:Connection',
            'celery.worker.consumer.mingle:Mingle',
            'celery.worker.consumer.events:Events',
            'celery.worker.consumer.gossip:Gossip',
            'celery.worker.consumer.heart:Heart',
            'celery.worker.consumer.control:Control',
            'celery.worker.consumer.tasks:Tasks',
            'celery.worker.consumer.consumer:Evloop',
            'celery.worker.consumer.agent:Agent',
        ]

        async def shutdown(self, parent: WorkerConsumerT) -> None:
            await self.send_all(parent, 'shutdown')

    def __init__(self,
                 on_task_request: Callable,
                 *,
                 init_callback: Callable = noop,
                 hostname: str = None,
                 pool: PoolT = None,
                 app: AppT = None,
                 timer: TimerT = None,
                 controller: WorkerT = None,
                 hub: LoopT = None,
                 amqheartbeat: float = None,
                 worker_options: Mapping = None,
                 disable_rate_limits: bool = False,
                 initial_prefetch_count: int = 2,
                 prefetch_multiplier: int = 1,
                 **kwargs) -> None:
        self.app = app
        self.controller = controller
        self.init_callback = init_callback
        self.hostname = hostname or gethostname()
        self.pid = os.getpid()
        self.pool = pool
        self.timer = timer
        self.strategies = self.Strategies()
        self.conninfo = self.app.connection_for_read()
        self.connection_errors = self.conninfo.connection_errors
        self.channel_errors = self.conninfo.channel_errors
        self._restart_state = restart_state(maxR=5, maxT=1)

        self._does_info = logger.isEnabledFor(logging.INFO)
        self._limit_order = 0
        self.on_task_request = on_task_request
        self.on_task_message = set()
        self.amqheartbeat_rate = self.app.conf.broker_heartbeat_checkrate
        self.disable_rate_limits = disable_rate_limits
        self.initial_prefetch_count = initial_prefetch_count
        self.prefetch_multiplier = prefetch_multiplier

        # this contains a tokenbucket for each task type by name, used for
        # rate limits, or None if rate limits are disabled for that task.
        self.task_buckets = defaultdict(lambda: None)
        self.reset_rate_limits()

        self.hub = hub
        if self.hub or getattr(self.pool, 'is_green', False):
            self.amqheartbeat = amqheartbeat
            if self.amqheartbeat is None:
                self.amqheartbeat = self.app.conf.broker_heartbeat
        else:
            self.amqheartbeat = 0

        if not hasattr(self, 'loop'):
            self.loop = loops.asynloop if hub else loops.synloop

        if _detect_environment() == 'gevent':
            # there's a gevent bug that causes timeouts to not be reset,
            # so if the connection timeout is exceeded once, it can NEVER
            # connect again.
            self.app.conf.broker_connection_timeout = None

        self._pending_operations = []

        self.steps = []
        self.blueprint = self.Blueprint(
            steps=self.app.steps['consumer'],
            on_close=self.on_close,
        )
        self.blueprint.apply(self, **dict(worker_options or {}, **kwargs))

    def call_soon(self, p: Thenable, *args, **kwargs) -> Thenable:
        p = ppartial(p, *args, **kwargs)
        if self.hub:
            return self.hub.call_soon(p)
        self._pending_operations.append(p)
        return p

    def perform_pending_operations(self) -> None:
        if not self.hub:
            while self._pending_operations:
                try:
                    self._pending_operations.pop()()
                except Exception as exc:  # pylint: disable=broad-except
                    logger.exception('Pending callback raised: %r', exc)

    def bucket_for_task(self, type: TaskT) -> TokenBucket:
        limit = rate(getattr(type, 'rate_limit', None))
        return TokenBucket(limit, capacity=1) if limit else None

    def reset_rate_limits(self) -> None:
        self.task_buckets.update(
            (n, self.bucket_for_task(t)) for n, t in self.app.tasks.items()
        )

    def _update_prefetch_count(self, index: int = 0) -> int:
        """Update prefetch count after pool/shrink grow operations.

        Index must be the change in number of processes as a positive
        (increasing) or negative (decreasing) number.

        Note:
            Currently pool grow operations will end up with an offset
            of +1 if the initial size of the pool was 0 (e.g.
            :option:`--autoscale=1,0 <celery worker --autoscale>`).
        """
        num_processes = self.pool.num_processes
        if not self.initial_prefetch_count or not num_processes:
            return  # prefetch disabled
        self.initial_prefetch_count = (
            self.pool.num_processes * self.prefetch_multiplier
        )
        return self._update_qos_eventually(index)

    def _update_qos_eventually(self, index: int) -> int:
        return (self.qos.decrement_eventually if index < 0
                else self.qos.increment_eventually)(
            abs(index) * self.prefetch_multiplier)

    async def _limit_move_to_pool(self, request: RequestT) -> None:
        task_reserved(request)
        await self.on_task_request(request)

    async def _on_bucket_wakeup(self, bucket: TokenBucket, tokens: int):
        try:
            request = bucket.pop()
        except IndexError:
            pass
        else:
            await self._limit_move_to_pool(request)
            self._schedule_oldest_bucket_request(bucket, tokens)

    def _schedule_oldest_bucket_request(
            self, bucket: TokenBucket, tokens: int) -> None:
        try:
            request = bucket.pop()
        except IndexError:
            pass
        else:
            self._schedule_bucket_request(request, bucket, tokens)

    def _schedule_bucket_request(
            self, request: RequestT, bucket: TokenBucket, tokens: int) -> None:
        bucket.can_consume(tokens)
        bucket.add(request)
        pri = self._limit_order = (self._limit_order + 1) % 10
        hold = bucket.expected_time(tokens)
        self.timer.call_after(
            hold, self._on_bucket_wakeup, (bucket, tokens),
            priority=pri,
        )

    def _limit_task(
            self, request: RequestT,
            bucket: TokenBucket,
            tokens: int) -> None:
        if bucket.contents:
            bucket.add(request)
        else:
            self._schedule_bucket_request(request, bucket, tokens)

<<<<<<< HEAD
    def _limit_post_eta(self, request, bucket, tokens):
        self.qos.decrement_eventually()
        if bucket.contents:
            return bucket.add(request)
        return self._schedule_bucket_request(request, bucket, tokens)

    def start(self):
=======
    async def start(self) -> None:
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
        blueprint = self.blueprint
        while blueprint.state not in STOP_CONDITIONS:
            maybe_shutdown()
            if self.restart_count:
                try:
                    self._restart_state.step()
                except RestartFreqExceeded as exc:
                    crit('Frequent restarts detected: %r', exc, exc_info=1)
                    sleep(1)
            self.restart_count += 1
            try:
                await blueprint.start(self)
            except self.connection_errors as exc:
                # If we're not retrying connections, no need to catch
                # connection errors
                if not self.app.conf.broker_connection_retry:
                    raise
                if isinstance(exc, OSError) and exc.errno == errno.EMFILE:
                    raise  # Too many open files
                maybe_shutdown()
                if blueprint.state not in STOP_CONDITIONS:
                    if self.connection:
                        self.on_connection_error_after_connected(exc)
                    else:
                        self.on_connection_error_before_connected(exc)
                    self.on_close()
                    await blueprint.restart(self)

    def on_connection_error_before_connected(self, exc: Exception) -> None:
        error(CONNECTION_ERROR, self.conninfo.as_uri(), exc,
              'Trying to reconnect...')

    def on_connection_error_after_connected(self, exc: Exception) -> None:
        warn(CONNECTION_RETRY, exc_info=True)
        try:
            self.connection.collect()
        except Exception:  # pylint: disable=broad-except
            pass

    async def register_with_event_loop(self, hub: LoopT) -> None:
        await self.blueprint.send_all(
            self, 'register_with_event_loop', args=(hub,),
            description='Hub.register',
        )

    async def shutdown(self) -> None:
        await self.blueprint.shutdown(self)

    async def stop(self) -> None:
        await self.blueprint.stop(self)

    def on_ready(self) -> None:
        callback, self.init_callback = self.init_callback, None
        if callback:
            callback(self)

    def loop_args(self) -> Tuple:
        return (self, self.connection, self.task_consumer,
                self.blueprint, self.hub, self.qos, self.amqheartbeat,
                self.app.clock, self.amqheartbeat_rate)

    async def on_decode_error(self, message: MessageT, exc: Exception) -> None:
        """Callback called if an error occurs while decoding a message.

        Simply logs the error and acknowledges the message so it
        doesn't enter a loop.

        Arguments:
            message (kombu.Message): The message received.
            exc (Exception): The exception being handled.
        """
        crit(MESSAGE_DECODE_ERROR,
             exc, message.content_type, message.content_encoding,
             safe_repr(message.headers), dump_body(message, message.body),
             exc_info=1)
        await message.ack()

    def on_close(self) -> None:
        # Clear internal queues to get rid of old messages.
        # They can't be acked anyway, as a delivery tag is specific
        # to the current channel.
        if self.controller and self.controller.semaphore:
            self.controller.semaphore.clear()
        if self.timer:
            self.timer.clear()
        for bucket in self.task_buckets.values():
            if bucket:
                bucket.clear_pending()
        reserved_requests.clear()
        if self.pool and self.pool.flush:
            self.pool.flush()

    async def connect(self) -> ConnectionT:
        """Establish the broker connection used for consuming tasks.

        Retries establishing the connection if the
        :setting:`broker_connection_retry` setting is enabled
        """
        conn = self.connection_for_read(heartbeat=self.amqheartbeat)
        await conn.connect()
        if self.hub:
            conn.transport.register_with_event_loop(conn.connection, self.hub)
        return conn

    async def connection_for_read(self,
                                  heartbeat: float = None) -> ConnectionT:
        return await self.ensure_connected(
            self.app.connection_for_read(heartbeat=heartbeat))

    async def connection_for_write(self,
                                   heartbeat: float = None) -> ConnectionT:
        return await self.ensure_connected(
            self.app.connection_for_write(heartbeat=heartbeat))

    async def ensure_connected(self, conn: ConnectionT) -> ConnectionT:
        # Callback called for each retry while the connection
        # can't be established.
        def _error_handler(exc, interval, next_step=CONNECTION_RETRY_STEP):
            if getattr(conn, 'alt', None) and interval == 0:
                next_step = CONNECTION_FAILOVER
            error(CONNECTION_ERROR, conn.as_uri(), exc,
                  next_step.format(when=humanize_seconds(interval, 'in', ' ')))

        # remember that the connection is lazy, it won't establish
        # until needed.
        if not self.app.conf.broker_connection_retry:
            # retry disabled, just call connect directly.
            await conn.connect()
            return conn

        conn = await conn.ensure_connection(
            _error_handler, self.app.conf.broker_connection_max_retries,
            callback=maybe_shutdown,
        )
        return conn

    def _flush_events(self) -> None:
        if self.event_dispatcher:
            self.event_dispatcher.flush()

    def on_send_event_buffered(self) -> None:
        if self.hub:
            self.hub._ready.add(self._flush_events)

    async def add_task_queue(self, queue: str,
                             exchange: str = None,
                             exchange_type: str = None,
                             routing_key: str = None,
                             **options) -> None:
        cset = self.task_consumer
        queues = self.app.amqp.queues
        # Must use in' here, as __missing__ will automatically
        # create queues when :setting:`task_create_missing_queues` is enabled.
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
            await cset.consume()
            info('Started consuming from %s', queue)

    async def cancel_task_queue(self, queue):
        info('Canceling queue %s', queue)
        self.app.amqp.queues.deselect(queue)
        await self.task_consumer.cancel_by_queue(queue)

    def apply_eta_task(self, request: RequestT) -> None:
        """Method called by the timer to apply a task with an ETA/countdown."""
        task_reserved(request)
        self.on_task_request(request)
        self.qos.decrement_eventually()

    def _message_report(self, body: bytes, message: MessageT) -> str:
        return MESSAGE_REPORT.format(dump_body(message, body),
                                     safe_repr(message.content_type),
                                     safe_repr(message.content_encoding),
                                     safe_repr(message.delivery_info),
                                     safe_repr(message.headers))

    async def on_unknown_message(self, body: bytes, message: MessageT) -> None:
        warn(UNKNOWN_FORMAT, self._message_report(body, message))
        await message.reject_log_error(logger, self.connection_errors)
        signals.task_rejected.send(sender=self, message=message, exc=None)

    async def on_unknown_task(
            self, body: bytes, message: MessageT, exc: Exception) -> None:
        error(UNKNOWN_TASK_ERROR, exc, dump_body(message, body), exc_info=True)
        try:
            id_, name = message.headers['id'], message.headers['task']
            root_id = message.headers.get('root_id')
        except KeyError:  # proto1
            payload = message.payload
            id_, name = payload['id'], payload['task']
            root_id = None
        request = Bunch(
            name=name, chord=None, root_id=root_id,
            correlation_id=message.properties.get('correlation_id'),
            reply_to=message.properties.get('reply_to'),
            errbacks=None,
        )
        await message.reject_log_error(logger, self.connection_errors)
        await self.app.backend.mark_as_failure(
            id_, NotRegistered(name), request=request,
        )
        if self.event_dispatcher:
            await self.event_dispatcher.send(
                'task-failed', uuid=id_,
                exception='NotRegistered({0!r})'.format(name),
            )
        signals.task_unknown.send(
            sender=self, message=message, exc=exc, name=name, id=id_,
        )

    async def on_invalid_task(
            self, body: bytes, message: MessageT, exc: Exception) -> None:
        error(INVALID_TASK_ERROR, exc, dump_body(message, body), exc_info=True)
        await message.reject_log_error(logger, self.connection_errors)
        signals.task_rejected.send(sender=self, message=message, exc=exc)

    def update_strategies(self) -> None:
        loader = self.app.loader
        for name, task in self.app.tasks.items():
            self.strategies[name] = task.start_strategy(self.app, self)
            task.__trace__ = build_tracer(name, task, loader, self.hostname,
                                          app=self.app)

    def create_task_handler(self) -> Callable:
        strategies = self.strategies
        on_unknown_message = self.on_unknown_message
        on_unknown_task = self.on_unknown_task
        on_invalid_task = self.on_invalid_task
        callbacks = self.on_task_message
        call_soon = self.call_soon
        promise_t = promise

        async def on_task_received(message: MessageT) -> None:
            # payload will only be set for v1 protocol, since v2
            # will defer deserializing the message body to the pool.
            payload = None
            try:
                type_ = message.headers['task']                # protocol v2
            except TypeError:
                return on_unknown_message(None, message)
            except KeyError:
                try:
                    payload = message.decode()
                except Exception as exc:  # pylint: disable=broad-except
                    return await self.on_decode_error(message, exc)
                try:
                    type_, payload = payload['task'], payload  # protocol v1
                except (TypeError, KeyError):
                    return await on_unknown_message(payload, message)
            try:
                strategy = strategies[type_]
            except KeyError as exc:
                return await on_unknown_task(None, message, exc)
            else:
                try:
                    await strategy(
                        message, payload,
                        promise_t(call_soon, (message.ack_log_error,)),
                        promise_t(call_soon, (message.reject_log_error,)),
                        callbacks,
                    )
                except InvalidTaskError as exc:
                    return await on_invalid_task(payload, message, exc)

        return on_task_received

    def __repr__(self) -> str:
        """``repr(self)``."""
        return '<Consumer: {self.hostname} ({state})>'.format(
            self=self, state=self.blueprint.human_state(),
        )


class Evloop(bootsteps.StartStopStep):
    """Event loop service.

    Note:
        This is always started last.
    """

    label = 'event loop'
    last = True

    async def start(self, c: WorkerConsumerT) -> None:
        self.patch_all(c)
        c.loop(*c.loop_args())

    def patch_all(self, c: WorkerConsumerT) -> None:
        c.qos._mutex = DummyLock()
