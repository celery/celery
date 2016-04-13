from __future__ import absolute_import, unicode_literals

from celery import bootsteps
from celery.utils.log import get_logger

from celery.worker import pidbox

from .tasks import Tasks

__all__ = ['Control']
logger = get_logger(__name__)


class Control(bootsteps.StartStopStep):

    requires = (Tasks,)

    def __init__(self, c, **kwargs):
        self.is_green = c.pool is not None and c.pool.is_green
        self.box = (pidbox.gPidbox if self.is_green else pidbox.Pidbox)(c)
        self.start = self.box.start
        self.stop = self.box.stop
        self.shutdown = self.box.shutdown

    def include_if(self, c):
        return (c.app.conf.worker_enable_remote_control and
                c.conninfo.supports_exchange_type('fanout'))
