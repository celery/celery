"""Couchbase result store backend."""

from kombu.utils.url import _parse_url

from celery.exceptions import ImproperlyConfigured

from .base import KeyValueStoreBackend

try:
    from couchbase.auth import PasswordAuthenticator
    from couchbase.cluster import Cluster
except ImportError:
    Cluster = PasswordAuthenticator = None

try:
    from couchbase.exceptions import DocumentNotFoundException
except ImportError:  # pragma: no cover
    class DocumentNotFoundException(Exception):
        """Stand-in so ``except`` clauses stay valid without the SDK."""

try:
    from couchbase.options import GetMultiOptions
except ImportError:  # pragma: no cover
    GetMultiOptions = None

try:
    from couchbase_core._libcouchbase import FMT_AUTO
except ImportError:
    FMT_AUTO = None

__all__ = ('CouchbaseBackend',)


class CouchbaseBackend(KeyValueStoreBackend):
    """Couchbase backend.

    Raises:
        celery.exceptions.ImproperlyConfigured:
            if module :pypi:`couchbase` is not available.
    """

    bucket = 'default'
    host = 'localhost'
    port = 8091
    username = None
    password = None
    quiet = False
    supports_autoexpire = True

    timeout = 2.5

    # Use str as couchbase key not bytes
    key_t = str

    def __init__(self, url=None, *args, **kwargs):
        kwargs.setdefault('expires_type', int)
        super().__init__(*args, **kwargs)
        self.url = url

        if Cluster is None:
            raise ImproperlyConfigured(
                'You need to install the couchbase library to use the '
                'Couchbase backend.',
            )

        uhost = uport = uname = upass = ubucket = None
        if url:
            _, uhost, uport, uname, upass, ubucket, _ = _parse_url(url)
            ubucket = ubucket.strip('/') if ubucket else None

        config = self.app.conf.get('couchbase_backend_settings', None)
        if config is not None:
            if not isinstance(config, dict):
                raise ImproperlyConfigured(
                    'Couchbase backend settings should be grouped in a dict',
                )
        else:
            config = {}

        self.host = uhost or config.get('host', self.host)
        self.port = int(uport or config.get('port', self.port))
        self.bucket = ubucket or config.get('bucket', self.bucket)
        self.username = uname or config.get('username', self.username)
        self.password = upass or config.get('password', self.password)

        self._connection = None

    def _get_connection(self):
        """Connect to the Couchbase server."""
        if self._connection is None:
            if self.host and self.port:
                uri = f"couchbase://{self.host}:{self.port}"
            else:
                uri = f"couchbase://{self.host}"
            if self.username and self.password:
                opt = PasswordAuthenticator(self.username, self.password)
            else:
                opt = None

            cluster = Cluster(uri, opt)

            bucket = cluster.bucket(self.bucket)

            self._connection = bucket.default_collection()
        return self._connection

    @property
    def connection(self):
        return self._get_connection()

    def get(self, key):
        try:
            return self.connection.get(key).content
        except DocumentNotFoundException:
            # A missing document means no (or expired) result, not an error:
            # the KV backend contract is to return None for absent keys.
            return None

    def set(self, key, value):
        # Since 4.0.0 value is JSONType in couchbase lib, so parameter format isn't needed
        if FMT_AUTO is not None:
            self.connection.upsert(key, value, ttl=self.expires, format=FMT_AUTO)
        else:
            self.connection.upsert(key, value, ttl=self.expires)

    def mget(self, keys):
        # ``get_multi`` returns a MultiGetResult, which is neither a mapping
        # (no ``.items()``) nor iterable, so the KV contract's
        # ``_mget_to_results`` cannot consume it. By default it also raises
        # on the first failed key. Ask the SDK to keep per-key results, then
        # build the plain dict: a missing document means no result (None),
        # any other per-key failure is a real error and is re-raised.
        if GetMultiOptions is None:  # pragma: no cover - SDK without the option
            return self.connection.get_multi(keys)
        result = self.connection.get_multi(
            keys, GetMultiOptions(return_exceptions=True))
        values = {key: None for key in keys}
        for key, res in result.results.items():
            values[key] = res.content
        for exc in result.exceptions.values():
            if not isinstance(exc, DocumentNotFoundException):
                raise exc
        return values

    def delete(self, key):
        try:
            self.connection.remove(key)
        except DocumentNotFoundException:
            # Deleting an absent (already expired or forgotten) key is a
            # no-op, matching the other KV backends.
            pass
