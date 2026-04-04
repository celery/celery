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
        c.connection = c.connect()
        info('Connected to %s', c.connection.as_uri())

    def stop(self, c):
        # Called during blueprint.restart() on connection loss.
        # c.connection lives on the consumer, not on self.obj, so the
        # inherited StartStopStep.stop() (which guards on self.obj) would
        # be a no-op.  We must close it explicitly here so that the old,
        # broken socket is released before the reconnect attempt.
        connection, c.connection = c.connection, None
        if connection:
            ignore_errors(connection, connection.close)

    def shutdown(self, c):
        # We must set c.connection to None here, so
        # that the green pidbox thread exits.
        self.stop(c)

    def info(self, c):
        params = 'N/A'
        if c.connection:
            params = c.connection.info()
            params.pop('password', None)  # don't send password.
        return {'broker': params}
