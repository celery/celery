"""Consumer Broker Connection Bootstep."""
from typing import Mapping
from kombu.common import ignore_errors
from celery import bootsteps
from celery.types import WorkerConsumerT
from celery.utils.log import get_logger

__all__ = ['Connection']

logger = get_logger(__name__)
info = logger.info


class Connection(bootsteps.StartStopStep):
    """Service managing the consumer broker connection."""

    def __init__(self, c: WorkerConsumerT, **kwargs) -> None:
        c.connection = None
        super(Connection, self).__init__(c, **kwargs)

    async def start(self, c) -> None:
        c.connection = await c.connect()
        info('Connected to %s', c.connection.as_uri())

    async def shutdown(self, c) -> None:
        # We must set self.connection to None here, so
        # that the green pidbox thread exits.
        connection, c.connection = c.connection, None
        if connection:
            await ignore_errors(connection, connection.close)

    def info(self, c) -> Mapping:
        params = 'N/A'
        if c.connection:
            params = c.connection.info()
            params.pop('password', None)  # don't send password.
        return {'broker': params}
