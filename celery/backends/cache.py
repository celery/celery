"""celery.backends.cache"""
from django.utils.encoding import smart_str
from django.core.cache import cache, get_cache
from django.core.cache.backends.base import InvalidCacheBackendError

from celery import conf
from celery.backends.base import KeyValueStoreBackend

# CELERY_CACHE_BACKEND overrides the django-global(tm) backend settings.
if conf.CELERY_CACHE_BACKEND:
    cache = get_cache(conf.CELERY_CACHE_BACKEND)


class DjangoMemcacheWrapper(object):
    """Wrapper class to django's memcache backend class, that overrides the
    :meth:`get` method in order to remove the forcing of unicode strings
    since it may cause binary or pickled data to break."""

    def __init__(self, cache):
        self.cache = cache

    def get(self, key, default=None):
        val = self.cache._cache.get(smart_str(key))
        if val is None:
            return default
        else:
            return val

    def set(self, key, value, timeout=0):
        self.cache.set(key, value, timeout)

# Check if django is using memcache as the cache backend. If so, wrap the
# cache object in a DjangoMemcacheWrapper that fixes a bug with retrieving
# pickled data
try:
    from django.core.cache.backends.memcached import CacheClass
    if isinstance(cache, CacheClass):
        cache = DjangoMemcacheWrapper(cache)
except InvalidCacheBackendError:
    pass


class Backend(KeyValueStoreBackend):
    """Backend using the Django cache framework to store task metadata."""

    def get(self, key):
        return cache.get(key)

    def set(self, key, value):
        cache.set(key, value)
