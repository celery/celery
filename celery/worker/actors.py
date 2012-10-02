from __future__ import absolute_import

from celery.bootsteps import StartStopStep
from celery.utils.imports import instantiate, qualname
from celery.utils.log import get_logger
from celery.worker.consumer import Connection
from cell import Actor
from kombu.common import ignore_errors
from kombu.utils import uuid

logger = get_logger(__name__)
debug, warn, error = logger.debug, logger.warn, logger.error


class Bootstep(StartStopStep):
    requires = (Connection, )

    def __init__(self, c, **kwargs):
        c.agent = None

    def create(self, c):
        agent = c.app.conf.CELERYD_AGENT
        agent = c.agent = self.instantiate(c.app.conf.CELERYD_AGENT,
            connection=c.connection, app=c.app,
        )
        return agent
