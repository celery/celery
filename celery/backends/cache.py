"""Memcached and in-memory cache result backend."""
from kombu.utils.encoding import bytes_to_str, ensure_bytes
from kombu.utils.objects import cached_property

from celery.exceptions import ImproperlyConfigured
from celery.utils.functional import LRUCache

from .base import KeyValueStoreBackend

__all__ = ('CacheBackend',)

REQUIRES_BACKEND = """\
The Memcached backend requires pymemcache.\
"""

UNKNOWN_BACKEND = """\
The cache backend {0!r} is unknown,
Please use one of the following backends instead: {1}\
"""

# Global shared in-memory cache for in-memory cache client
# This is to share cache between threads
_DUMMY_CLIENT_CACHE = LRUCache(limit=5000)


def get_memcache_client():
    """Get pymemcache client factory."""
    try:
        from pymemcache.client.base import Client
        from pymemcache.client.hash import HashClient
        from pymemcache.client.retrying import RetryingClient
    except ImportError:
        raise ImproperlyConfigured(REQUIRES_BACKEND)

    def ClientFactory(servers=None, **kwargs):
        """Create a pymemcache client with optional retry support.

        Args:
            servers: List of server addresses
            **kwargs: Additional options including:
                - retry_attempts: Number of retry attempts (enables RetryingClient)
                - retry_delay: Delay between retries in seconds
                - retry_for: List of exceptions to retry for
                - do_not_retry_for: List of exceptions to not retry for
                - behaviors: Ignored for backward compatibility with pylibmc
        """
        # Remove pylibmc-specific options for backward compatibility
        kwargs.pop('behaviors', None)

        # Extract retry-related options
        retry_attempts = kwargs.pop('retry_attempts', None)
        retry_delay = kwargs.pop('retry_delay', None)
        retry_for = kwargs.pop('retry_for', None)
        do_not_retry_for = kwargs.pop('do_not_retry_for', None)

        # Use HashClient for multiple servers, base Client for single server
        if len(servers) > 1:
            base_client = HashClient(servers, **kwargs)
        else:
            # For single server, use base Client with first server
            base_client = Client(servers[0], **kwargs)

        # Wrap with RetryingClient if retry options are specified
        if retry_attempts is not None:
            retry_kwargs = {'attempts': retry_attempts}
            if retry_delay is not None:
                retry_kwargs['retry_delay'] = retry_delay
            if retry_for is not None:
                retry_kwargs['retry_for'] = retry_for
            if do_not_retry_for is not None:
                retry_kwargs['do_not_retry_for'] = do_not_retry_for
            return RetryingClient(base_client, **retry_kwargs)

        return base_client

    return ClientFactory, bytes_to_str


class DummyClient:

    def __init__(self, *args, **kwargs):
        self.cache = _DUMMY_CLIENT_CACHE

    def get(self, key, *args, **kwargs):
        return self.cache.get(key)

    def get_multi(self, keys):
        cache = self.cache
        return {k: cache[k] for k in keys if k in cache}

    def set(self, key, value, *args, **kwargs):
        self.cache[key] = value

    def delete(self, key, *args, **kwargs):
        self.cache.pop(key, None)

    def incr(self, key, delta=1):
        return self.cache.incr(key, delta)

    def touch(self, key, expire):
        pass


backends = {
    'memcache': get_memcache_client,
    'memcached': get_memcache_client,
    'pylibmc': get_memcache_client,  # Backward compatibility
    'pymemcache': get_memcache_client,
    'memory': lambda: (DummyClient, ensure_bytes),
}


class CacheBackend(KeyValueStoreBackend):
    """Cache result backend."""

    servers = None
    supports_autoexpire = True
    supports_native_join = True
    implements_incr = True

    def __init__(self, app, expires=None, backend=None,
                 options=None, url=None, **kwargs):
        options = {} if not options else options
        super().__init__(app, **kwargs)
        self.url = url

        self.options = dict(self.app.conf.cache_backend_options,
                            **options)

        self.backend = url or backend or self.app.conf.cache_backend
        if self.backend:
            self.backend, _, servers = self.backend.partition('://')
            self.servers = servers.rstrip('/').split(';')
        self.expires = self.prepare_expires(expires, type=int)
        try:
            self.Client, self.key_t = backends[self.backend]()
        except KeyError:
            raise ImproperlyConfigured(UNKNOWN_BACKEND.format(
                self.backend, ', '.join(backends)))
        self._encode_prefixes()  # rencode the keyprefixes

    def get(self, key):
        return self.client.get(key)

    def mget(self, keys):
        return self.client.get_multi(keys)

    def set(self, key, value):
        return self.client.set(key, value, self.expires)

    def delete(self, key):
        return self.client.delete(key)

    def _apply_chord_incr(self, header_result_args, body, **kwargs):
        chord_key = self.get_key_for_chord(header_result_args[0])
        self.client.set(chord_key, 0, time=self.expires)
        return super()._apply_chord_incr(
            header_result_args, body, **kwargs)

    def incr(self, key):
        return self.client.incr(key)

    def expire(self, key, value):
        return self.client.touch(key, value)

    @cached_property
    def client(self):
        return self.Client(self.servers, **self.options)

    def __reduce__(self, args=(), kwargs=None):
        kwargs = {} if not kwargs else kwargs
        servers = ';'.join(self.servers)
        backend = f'{self.backend}://{servers}/'
        kwargs.update(
            {'backend': backend,
             'expires': self.expires,
             'options': self.options})
        return super().__reduce__(args, kwargs)

    def as_uri(self, *args, **kwargs):
        """Return the backend as an URI.

        This properly handles the case of multiple servers.
        """
        servers = ';'.join(self.servers)
        return f'{self.backend}://{servers}/'
