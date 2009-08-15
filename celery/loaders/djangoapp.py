from celery.loaders.base import BaseLoader


class Loader(BaseLoader):
    """The Django loader."""

    def read_configuration(self):
        """Load configuration from Django settings."""
        from django.conf import settings
        return settings

    def on_task_init(self, task_id, task):
        """This method is called before a task is executed.
        
        Does everything necessary for Django to work in a long-living,
        multiprocessing environment.
        
        """
        # See: http://groups.google.com/group/django-users/browse_thread/
        #       thread/78200863d0c07c6d/38402e76cf3233e8?hl=en&lnk=gst&
        #       q=multiprocessing#38402e76cf3233e8
        from django.db import connection
        connection.close()

        # Reset cache connection only if using memcached/libmemcached
        from django.core import cache
        # XXX At Opera we use a custom memcached backend that uses libmemcached
        # instead of libmemcache (cmemcache). Should find a better solution for
        # this, but for now "memcached" should probably be unique enough of a
        # string to not make problems.
        cache_backend = cache.settings.CACHE_BACKEND
        if hasattr(cache, "parse_backend_uri"):
            cache_scheme = cache.parse_backend_uri(cache_backend)[0]
        else:
            # Django <= 1.0.2
            cache_scheme = cache_backend.split(":", 1)[0]
        if "memcached" in cache_scheme:
            cache.cache.close()

    def on_worker_init(self):
        """Called when the worker starts.

        Uses :func:`celery.discovery.autodiscover` to automatically discover
        any ``tasks.py`` files in the applications listed in
        ``INSTALLED_APPS``.

        """
        from celery.discovery import autodiscover
        autodiscover()
