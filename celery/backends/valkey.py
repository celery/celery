"""Valkey result store backend."""
from .base_keyval import BaseKeyValueBackend, BaseSentinelBackend

try:
    import valkey.connection
    from valkey._parsers.url_parser import URL_QUERY_ARGUMENT_PARSERS
except ImportError:
    valkey = None
    URL_QUERY_ARGUMENT_PARSERS = None

try:
    import valkey.sentinel
except ImportError:
    pass

__all__ = ('ValkeyBackend', 'SentinelBackend')


class ValkeyBackend(BaseKeyValueBackend):
    """Valkey task result store.

    It makes use of the following commands:
    GET, MGET, DEL, INCRBY, EXPIRE, SET, SETEX
    """
    #: :pypi:`valkey` client module.
    backend = valkey
    backend_module = valkey
    if backend_module:
        backend_module.connection.URL_QUERY_ARGUMENT_PARSERS = URL_QUERY_ARGUMENT_PARSERS
    backend_name = "valkey"

    #: Maximal length of string value in Valkey.
    #: 512 MB - https://valkey.io/topics/data-types
    _MAX_STR_VALUE_SIZE = 536870912


if getattr(valkey, "sentinel", None):
    class SentinelManagedSSLConnection(
            valkey.sentinel.SentinelManagedConnection,
            valkey.SSLConnection):
        """Connect to a Valkey server using Sentinel + TLS.

        Use Sentinel to identify which Valkey server is the current master
        to connect to and when connecting to the Master server, use an
        SSL Connection.
        """


class SentinelBackend(BaseSentinelBackend, ValkeyBackend):
    """Valkey sentinel task result store."""

    sentinel = getattr(valkey, "sentinel", None)
    connection_class_ssl = SentinelManagedSSLConnection if sentinel else None
