"""celery.backends.tyrant"""
try:
    import pytyrant
except ImportError:
    pytyrant = None

from celery.backends.base import KeyValueStoreBackend
from celery.exceptions import ImproperlyConfigured


class TyrantBackend(KeyValueStoreBackend):
    """Tokyo Cabinet based task backend store.

    .. attribute:: tyrant_host

        The hostname to the Tokyo Tyrant server.

    .. attribute:: tyrant_port

        The port to the Tokyo Tyrant server.

    """
    tyrant_host = None
    tyrant_port = None

    def __init__(self, tyrant_host=None, tyrant_port=None, **kwargs):
        """Initialize Tokyo Tyrant backend instance.

        Raises :class:`celery.exceptions.ImproperlyConfigured` if
        :setting:`TT_HOST` or :setting:`TT_PORT` is not set.

        """
        super(TyrantBackend, self).__init__(**kwargs)

        if not pytyrant:
            raise ImproperlyConfigured(
                    "You need to install the pytyrant library to use the "
                  + "Tokyo Tyrant backend.")
        self.tyrant_host = (tyrant_host or
                            self.app.conf.get("TT_HOST") or
                            self.tyrant_host)
        self.tyrant_port = (tyrant_port or
                            self.app.conf.get("TT_PORT") or
                            self.tyrant_port)
        if self.tyrant_port:
            self.tyrant_port = int(self.tyrant_port)
        if not self.tyrant_host or not self.tyrant_port:
            raise ImproperlyConfigured(
                "To use the Tokyo Tyrant backend, you have to "
                "set the TT_HOST and TT_PORT settings in your settings.py")
        self._connection = None

    def open(self):
        """Get :class:`pytyrant.PyTyrant` instance with the current
        server configuration.

        The connection is then cached until you do an
        explicit :meth:`close`.

        """
        # connection overrides bool()
        if self._connection is None:
            self._connection = pytyrant.PyTyrant.open(self.tyrant_host,
                                                      self.tyrant_port)
        return self._connection

    def close(self):
        """Close the tyrant connection and remove the cache."""
        # connection overrides bool()
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def process_cleanup(self):
        self.close()

    def get(self, key):
        return self.open().get(key)

    def set(self, key, value):
        self.open()[key] = value

    def delete(self, key):
        self.open().pop(key, None)
