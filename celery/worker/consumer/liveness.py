"""Worker liveness file.

With :setting:`worker_liveness_file` set, the worker touches that file
every :setting:`worker_liveness_interval` seconds while it can still do
its work, so a container health check only has to read the file's age.
"""
import os
from time import monotonic

from kombu import Consumer, Exchange, Producer, Queue
from kombu.common import ignore_errors
from kombu.utils.uuid import uuid

from celery import bootsteps
from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger
from celery.utils.nodenames import node_format
from celery.worker import state

from .tasks import Tasks

__all__ = ('Liveness',)

logger = get_logger(__name__)
debug, warning = logger.debug, logger.warning

#: Transports whose event loop reads the probe back from the broker.
PROBE_TRANSPORTS = frozenset({'amqp', 'redis'})

#: Don't follow a symlink planted at the path, for example in a shared /tmp,
#: and don't block on a FIFO planted there.
TOUCH_FLAGS = (os.O_WRONLY | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0) |
               getattr(os, 'O_NONBLOCK', 0))


def _channel_is_open(channel):
    if channel is None:
        return False
    if hasattr(channel, 'is_open'):  # py-amqp
        return bool(channel.is_open)
    return not getattr(channel, 'closed', True)  # kombu virtual transports


class Liveness(bootsteps.StartStopStep):
    """Bootstep that keeps the liveness file fresh while the worker works.

    On RabbitMQ and Redis the step sends itself a small probe through the
    broker every interval, on a channel of its own, and touches the file
    when the worker's event loop reads the probe back.  On other transports
    it touches the file on the worker's timer.  Either way the task consumer
    must be open, and on the prefork pool no task may have run past its hard
    time limit by more than about an interval.
    """

    requires = (Tasks,)

    def __init__(self, c, liveness_file=None, **kwargs):
        self.path = node_format(
            liveness_file or c.app.conf.worker_liveness_file, c.hostname)
        self.interval = c.app.conf.worker_liveness_interval
        if self.path and not (self.interval or 0) > 0:
            raise ImproperlyConfigured(
                'worker_liveness_interval must be greater than zero.')
        # Probes carry this id, so a probe's monotonic send time is only
        # compared with the clock of the process that sent it.
        self.publisher_id = uuid()
        self.c = self.channel = self.consumer = self.producer = None
        self.exchange = self.routing_key = None
        self.tref = self.first_tref = None
        self.enforces_limits = False
        self.overrun = None
        self._active_since = {}
        self._touch_failed = False
        super().__init__(c, **kwargs)

    def include_if(self, c):
        return bool(self.path)

    def create(self, c):
        # A file left by an earlier process must not pass the check before
        # this worker has connected.
        try:
            os.unlink(self.path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise ImproperlyConfigured(
                f'Cannot remove the old worker_liveness_file {self.path!r}: '
                f'{exc.strerror or exc}') from exc

    def start(self, c):
        from celery.concurrency.prefork import TaskPool
        self.c = c
        self.enforces_limits = isinstance(c.pool, TaskPool)
        driver = c.connection.transport.driver_type
        # The probe is published from the timer, which must run on the event
        # loop's thread: the event loop's hub, or a green pool's greenlets.
        loop_timer = (getattr(c, 'hub', None) is not None or
                      getattr(c.pool, 'is_green', False))
        if driver not in PROBE_TRANSPORTS or not loop_timer:
            self.tref = c.timer.call_repeatedly(self.interval, self.check)
            self.check()
            return

        # A new name for each connection: RabbitMQ can keep an exclusive queue
        # for a while after the connection that declared it is gone.
        name = f'{c.hostname}.{uuid()}.celery.liveness'
        if driver == 'redis':
            # Redis reads a fanout queue on the channel's pub/sub connection,
            # so the probe adds no polling.  The expiry lets kombu remove the
            # binding left by a worker that was killed.
            self.exchange = Exchange(name, type='fanout', durable=False,
                                     auto_delete=True)
            self.routing_key = ''
            queue = Queue(name, exchange=self.exchange, routing_key='',
                          durable=False, exclusive=True, auto_delete=True,
                          expires=self.interval * 4)
        else:
            self.exchange = Exchange('')
            self.routing_key = name
            queue = Queue(name, exchange=self.exchange, routing_key=name,
                          durable=False, exclusive=True, auto_delete=True,
                          expires=self.interval * 4,
                          message_ttl=self.interval * 2)
        self.channel = c.connection.channel()
        self.consumer = Consumer(self.channel, [queue],
                                 callbacks=[self.on_probe],
                                 on_decode_error=self.on_decode_error,
                                 no_ack=True, accept=['json'])
        self.consumer.consume()
        self.producer = Producer(self.channel, serializer='json')
        self.tref = c.timer.call_repeatedly(self.interval, self.send_probe)
        if driver == 'redis':
            # The channel subscribes on the event loop's next pass and Redis
            # drops messages nobody is subscribed to yet, so wait a moment.
            self.first_tref = c.timer.call_after(
                min(1.0, self.interval / 2), self.send_probe)
        else:
            self.send_probe()

    def stop(self, c):
        for tref in (self.tref, self.first_tref):
            if tref is not None:
                tref.cancel()
        self.tref = self.first_tref = None
        if self.consumer is not None:
            ignore_errors(c, self.consumer.cancel)
            self.consumer = None
        if self.channel is not None:
            ignore_errors(c, self.channel.close)
            self.channel = None
    shutdown = stop

    def check(self):
        """Touch the file from the timer (transports without the probe)."""
        self.check_time_limits()
        if self.healthy():
            self.touch()

    def send_probe(self):
        self.check_time_limits()
        if self.waiting_for_green_pool() and self.healthy():
            # The event loop is handing a task to a full gevent or eventlet
            # pool, which blocks until a task finishes, so it can't read the
            # probe meanwhile.  The worker is busy, not stuck.
            self.touch()
        try:
            self.producer.publish(
                {'publisher_id': self.publisher_id, 'sent': monotonic()},
                exchange=self.exchange.name, routing_key=self.routing_key,
                retry=False,
            )
        except Exception as exc:  # the broker can't be reached: go stale
            debug('Liveness probe not sent: %r', exc)

    def on_probe(self, body, message):
        if not isinstance(body, dict) or body.get('publisher_id') != self.publisher_id:
            return
        sent = body.get('sent')
        if not isinstance(sent, (int, float)) or monotonic() - sent > 2 * self.interval:
            return  # a probe that took this long proves nothing
        if self.healthy():
            self.touch()

    def on_decode_error(self, message, exc):
        debug('Ignoring a message on the liveness queue: %r', exc)

    def waiting_for_green_pool(self):
        # A task is reserved before it's handed to the pool, so a full pool
        # with a reserved task that hasn't started means the hand-over waits.
        pool = self.c.pool
        if not getattr(pool, 'is_green', False):
            return False
        active = state.active_requests
        return (len(active) >= pool.num_processes and
                any(request not in active for request in state.reserved_requests))

    def healthy(self):
        task_consumer = self.c.task_consumer
        return (self.overrun is None and task_consumer is not None and
                _channel_is_open(task_consumer.channel))

    def check_time_limits(self):
        """Note a task that has run past its hard time limit.

        Only the prefork pool enforces hard time limits, so a task still
        running an interval after its limit means the pool lost track of it.
        Running time counts from when this step first saw the task, on the
        monotonic clock: ``Request.time_start`` is wall-clock time, and a
        clock change must not fail the check.  So a task is caught between
        one and three intervals after its limit.
        """
        if not self.enforces_limits:
            return
        now = monotonic()
        default_limit = getattr(self.c.controller, 'time_limit', None)
        try:
            requests = list(state.active_requests)
        except RuntimeError:  # changed by another thread: try next time
            return
        seen, self._active_since = self._active_since, {}
        overrun = None
        for request in requests:
            since = self._active_since[request.id] = seen.get(request.id, now)
            limit = (request.time_limits[0] or request.task.time_limit or
                     default_limit)
            if limit and now - since > limit + self.interval:
                overrun = f'{request.name}[{request.id}]'
        if overrun and overrun != self.overrun:
            warning('Liveness check failing: task %s is still running after '
                    'its hard time limit.', overrun)
        self.overrun = overrun

    def touch(self):
        try:
            fd = os.open(self.path, TOUCH_FLAGS, 0o644)
            try:
                os.utime(fd if os.utime in os.supports_fd else self.path)
            finally:
                os.close(fd)
        except OSError as exc:
            if not self._touch_failed:
                warning('Cannot update liveness file %r: %r', self.path, exc)
            self._touch_failed = True
        else:
            self._touch_failed = False
