"""Worker Remote Control Bootstep.

``Control`` -> :mod:`celery.worker.pidbox` -> :mod:`kombu.pidbox`.

The actual commands are implemented in :mod:`celery.worker.control`.
"""
from celery import bootsteps
from celery.utils.log import get_logger
from celery.types import WorkerConsumerT
from celery.worker import pidbox
from .tasks import Tasks

__all__ = ['Control']

logger = get_logger(__name__)


class Control(bootsteps.StartStopStep):
    """Remote control command service."""

    requires = (Tasks,)

    def __init__(self, c: WorkerConsumerT, **kwargs) -> None:
        self.is_green = c.pool is not None and c.pool.is_green
        self.box = (pidbox.gPidbox if self.is_green else pidbox.Pidbox)(c)
        self.start = self.box.start
        self.stop = self.box.stop
        self.shutdown = self.box.shutdown
        super(Control, self).__init__(c, **kwargs)

    def include_if(self, c: WorkerConsumerT) -> bool:
        return (c.app.conf.worker_enable_remote_control and
                c.conninfo.supports_exchange_type('fanout'))
