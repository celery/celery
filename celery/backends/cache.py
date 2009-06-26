"""celery.backends.cache"""
from django.core.cache import cache
from celery.backends.base import KeyValueStoreBackend


class Backend(KeyValueStoreBackend):
    """Backend using the Django cache framework to store task metadata."""

    def get(self, key):
        return cache.get(key)

    def set(self, key, value):
        cache.set(key, value)

