"""Consumer Broker Connection Bootstep."""
from kombu.common import ignore_errors

from celery import bootsteps
from celery.utils.log import get_logger

__all__ = ('Connection',)

logger = get_logger(__name__)
info = logger.info


class Connection(bootsteps.StartStopStep):
    """Service managing the consumer broker connection."""

    def __init__(self, c, **kwargs):
        c.connection = None
        super().__init__(c, **kwargs)

    def start(self, c):
        c.connection = c.connection
        c.connection = c.connect()
        for conn in c.connection:
            info('Connected to %s', conn.as_uri())

    def shutdown(self, c):
        # We must set self.connection to None here, so
        # that the green pidbox thread exits.
        connection, c.connection = c.connection, None
        if connection:
            for conn in connection:
                ignore_errors(conn, conn.close)

    def info(self, c):
        params = 'N/A'
        if c.connection:
            params = c.connection.info()
            params.pop('password', None)  # don't send password.
        return {'broker': params}
