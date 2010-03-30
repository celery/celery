import imp
import importlib

from celery.loaders.base import BaseLoader

_RACE_PROTECTION = False


class Loader(BaseLoader):
    """The Django loader."""
    _db_reuse = 0

    def read_configuration(self):
        """Load configuration from Django settings."""
        from django.conf import settings
        return settings

    def close_database(self):
        from django.db import connection
        db_reuse_max = getattr(self.conf, "CELERY_DB_REUSE_MAX", None)
        if not db_reuse_max:
            return connection.close()
        if self._db_reuse >= db_reuse_max:
            self._db_reuse = 0
            return connection.close()
        self._db_reuse += 1

    def on_task_init(self, task_id, task):
        """This method is called before a task is executed.

        Does everything necessary for Django to work in a long-living,
        multiprocessing environment.

        """
        # See http://groups.google.com/group/django-users/
        #            browse_thread/thread/78200863d0c07c6d/
        self.close_database()

        # ## Reset cache connection only if using memcached/libmemcached
        from django.core import cache
        # XXX At Opera we use a custom memcached backend that uses
        # libmemcached instead of libmemcache (cmemcache). Should find a
        # better solution for this, but for now "memcached" should probably
        # be unique enough of a string to not make problems.
        cache_backend = cache.settings.CACHE_BACKEND
        try:
            parse_backend = cache.parse_backend_uri
        except AttributeError:
            parse_backend = lambda backend: backend.split(":", 1)
        cache_scheme = parse_backend(cache_backend)[0]

        if "memcached" in cache_scheme:
            cache.cache.close()

    def on_worker_init(self):
        """Called when the worker starts.

        Automatically discovers any ``tasks.py`` files in the applications
        listed in ``INSTALLED_APPS``.

        """
        self.import_default_modules()
        autodiscover()


def autodiscover():
    """Include tasks for all applications in :setting:`INSTALLED_APPS`."""
    from django.conf import settings
    global _RACE_PROTECTION

    if _RACE_PROTECTION:
        return
    _RACE_PROTECTION = True
    try:
        return filter(None, [find_related_module(app, "tasks")
                                for app in settings.INSTALLED_APPS])
    finally:
        _RACE_PROTECTION = False


def find_related_module(app, related_name):
    """Given an application name and a module name, tries to find that
    module in the application."""

    try:
        app_path = importlib.import_module(app).__path__
    except AttributeError:
        return

    try:
        imp.find_module(related_name, app_path)
    except ImportError:
        return

    module = importlib.import_module("%s.%s" % (app, related_name))

    try:
        return getattr(module, related_name)
    except AttributeError:
        return
