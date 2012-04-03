from __future__ import absolute_import

import threading

default_app = None


class _TLS(threading.local):
    #: Apps with the :attr:`~celery.app.base.BaseApp.set_as_current` attribute
    #: sets this, so it will always contain the last instantiated app,
    #: and is the default app returned by :func:`app_or_default`.
    current_app = None

    #: The currently executing task.
    current_task = None
_tls = _TLS()


def set_default_app(app):
    global default_app
    default_app = app


def current_app():
    return getattr(_tls, "current_app", None) or default_app


def current_task():
    return getattr(_tls, "current_task", None)
