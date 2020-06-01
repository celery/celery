"""Django-specific customization."""
from __future__ import absolute_import, unicode_literals

import os
import sys
import warnings
from datetime import datetime
from importlib import import_module

from kombu.utils.imports import symbol_by_name
from kombu.utils.objects import cached_property

from celery import _state, signals
from celery.exceptions import FixupWarning, ImproperlyConfigured

__all__ = ('DjangoFixup', 'fixup')

ERR_NOT_INSTALLED = """\
Environment variable DJANGO_SETTINGS_MODULE is defined
but Django isn't installed.  Won't apply Django fix-ups!
"""


def _maybe_close_fd(fh):
    try:
        os.close(fh.fileno())
    except (AttributeError, OSError, TypeError):
        # TypeError added for celery#962
        pass


def _verify_django_version(django):
    if django.VERSION < (1, 11):
        raise ImproperlyConfigured('Celery 4.x requires Django 1.11 or later.')


def fixup(app, env='DJANGO_SETTINGS_MODULE'):
    """Install Django fixup if settings module environment is set."""
    SETTINGS_MODULE = os.environ.get(env)
    if SETTINGS_MODULE and 'django' not in app.loader_cls.lower():
        try:
            import django  # noqa
        except ImportError:
            warnings.warn(FixupWarning(ERR_NOT_INSTALLED))
        else:
            _verify_django_version(django)
            return DjangoFixup(app).install()


class DjangoFixup(object):
    """Fixup installed when using Django."""

    def __init__(self, app):
        self.app = app
        if _state.default_app is None:
            self.app.set_default()
        self._worker_fixup = None

    def install(self):
        # Need to add project directory to path.
        # The project directory has precedence over system modules,
        # so we prepend it to the path.
        sys.path.insert(0, os.getcwd())

        self._settings = symbol_by_name('django.conf:settings')
        self.app.loader.now = self.now

        signals.import_modules.connect(self.on_import_modules)
        signals.worker_init.connect(self.on_worker_init)
        return self

    @property
    def worker_fixup(self):
        if self._worker_fixup is None:
            self._worker_fixup = DjangoWorkerFixup(self.app)
        return self._worker_fixup

    @worker_fixup.setter
    def worker_fixup(self, value):
        self._worker_fixup = value

    def on_import_modules(self, **kwargs):
        # call django.setup() before task modules are imported
        self.worker_fixup.validate_models()

    def on_worker_init(self, **kwargs):
        self.worker_fixup.install()

    def now(self, utc=False):
        return datetime.utcnow() if utc else self._now()

    def autodiscover_tasks(self):
        from django.apps import apps
        return [config.name for config in apps.get_app_configs()]

    @cached_property
    def _now(self):
        return symbol_by_name('django.utils.timezone:now')


class DjangoWorkerFixup(object):
    _db_recycles = 0

    def __init__(self, app):
        self.app = app
        self.db_reuse_max = self.app.conf.get('CELERY_DB_REUSE_MAX', None)
        self._db = import_module('django.db')
        self._cache = import_module('django.core.cache')
        self._settings = symbol_by_name('django.conf:settings')

        self.interface_errors = (
            symbol_by_name('django.db.utils.InterfaceError'),
        )
        self.DatabaseError = symbol_by_name('django.db:DatabaseError')

    def django_setup(self):
        import django
        django.setup()

    def validate_models(self):
        from django.core.checks import run_checks
        self.django_setup()
        run_checks()

    def install(self):
        signals.beat_embedded_init.connect(self.close_database)
        signals.worker_ready.connect(self.on_worker_ready)
        signals.task_prerun.connect(self.on_task_prerun)
        signals.task_postrun.connect(self.on_task_postrun)
        signals.worker_process_init.connect(self.on_worker_process_init)
        self.close_database()
        self.close_cache()
        return self

    def on_worker_process_init(self, **kwargs):
        # Child process must validate models again if on Windows,
        # or if they were started using execv.
        if os.environ.get('FORKED_BY_MULTIPROCESSING'):
            self.validate_models()

        # close connections:
        # the parent process may have established these,
        # so need to close them.

        # calling db.close() on some DB connections will cause
        # the inherited DB conn to also get broken in the parent
        # process so we need to remove it without triggering any
        # network IO that close() might cause.
        for c in self._db.connections.all():
            if c and c.connection:
                self._maybe_close_db_fd(c.connection)

        # use the _ version to avoid DB_REUSE preventing the conn.close() call
        self._close_database(force=True)
        self.close_cache()

    def _maybe_close_db_fd(self, fd):
        try:
            _maybe_close_fd(fd)
        except self.interface_errors:
            pass

    def on_task_prerun(self, sender, **kwargs):
        """Called before every task."""
        if not getattr(sender.request, 'is_eager', False):
            self.close_database()

    def on_task_postrun(self, sender, **kwargs):
        # See https://groups.google.com/group/django-users/
        #            browse_thread/thread/78200863d0c07c6d/
        if not getattr(sender.request, 'is_eager', False):
            self.close_database()
            self.close_cache()

    def close_database(self, **kwargs):
        if not self.db_reuse_max:
            return self._close_database()
        if self._db_recycles >= self.db_reuse_max * 2:
            self._db_recycles = 0
            self._close_database()
        self._db_recycles += 1

    def _close_database(self, force=False):
        for conn in self._db.connections.all():
            try:
                if force:
                    conn.close()
                else:
                    conn.close_if_unusable_or_obsolete()
            except self.interface_errors:
                pass
            except self.DatabaseError as exc:
                str_exc = str(exc)
                if 'closed' not in str_exc and 'not connected' not in str_exc:
                    raise

    def close_cache(self):
        try:
            self._cache.close_caches()
        except (TypeError, AttributeError):
            pass

    def on_worker_ready(self, **kwargs):
        if self._settings.DEBUG:
            warnings.warn('''Using settings.DEBUG leads to a memory
            leak, never use this setting in production environments!''')
