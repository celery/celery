"""Consumer Broker Connection Bootstep."""
from kombu.common import ignore_errors
from kombu.utils.url import maybe_sanitize_url

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

    def close_connection(self, c):
        """Close and clear c.connection.

        Used by shutdown() for final cleanup. The error handler in
        on_connection_error_after_connected() performs the same close
        inline to release the broken socket before blueprint.restart().
        """
        connection, c.connection = c.connection, None
        if connection:
            ignore_errors(connection, connection.close)

    def shutdown(self, c):
        # We must set c.connection to None here, so
        # that the green pidbox thread exits.
        self.close_connection(c)

    def info(self, c):
        params = 'N/A'
        if c.connection:
            params = c.connection.info()
            params.pop('password', None)  # don't send password.
            alternates = params.get('alternates')
            if isinstance(alternates, str):
                params['alternates'] = maybe_sanitize_url(alternates)
            elif isinstance(alternates, (list, tuple)):
                params['alternates'] = [
                    maybe_sanitize_url(url) if isinstance(url, str) else url
                    for url in alternates
                ]
        return {'broker': params}
