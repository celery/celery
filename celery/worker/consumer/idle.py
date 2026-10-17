"""Worker Idle Shutdown Bootstep."""
from time import monotonic

from celery import bootsteps
from celery.platforms import EX_OK
from celery.utils.log import get_logger
from celery.worker import state

__all__ = ('IdleShutdown',)

logger = get_logger(__name__)


class IdleShutdown(bootsteps.StartStopStep):
    """Bootstep that shuts down the worker after it has been idle for too long."""

    label = 'idle shutdown'

    def __init__(self, c, max_idle=None, **kwargs):
        self.max_idle = c.app.either('worker_max_idle', max_idle)
        self.enabled = bool(self.max_idle)
        self.tref = None
        self.last_busy = None
        self.last_count = None
        super().__init__(c, **kwargs)

    def start(self, c):
        self.last_busy = monotonic()
        self.last_count = state.all_total_count[0]
        self.tref = c.timer.call_repeatedly(min(self.max_idle, 1.0), self.check)

    def stop(self, c):
        if self.tref is not None:
            self.tref.cancel()
            self.tref = None
    shutdown = stop

    def check(self):
        count = state.all_total_count[0]
        if (count != self.last_count or state.active_requests
                or state.reserved_requests or state.scheduled_requests):
            self.last_count = count
            self.last_busy = monotonic()
        elif monotonic() - self.last_busy >= self.max_idle:
            logger.info('Worker has been idle for %s seconds, shutting down.', self.max_idle)
            state.should_stop = EX_OK
            self.stop(None)
