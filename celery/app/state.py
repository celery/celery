from __future__ import absolute_import

import threading


class _TLS(threading.local):
    #: Apps with the :attr:`~celery.app.base.BaseApp.set_as_current` attribute
    #: sets this, so it will always contain the last instantiated app,
    #: and is the default app returned by :func:`app_or_default`.
    current_app = None

    #: The currently executing task.
    current_task = None
_tls = _TLS()


default_app = None


def set_default_app(app):
    global default_app
    default_app = app


def current_app():
    return getattr(_tls, "current_app", None) or default_app


def current_task():
    return getattr(_tls, "current_task", None)


def _app_or_default(app=None):
    """Returns the app provided or the default app if none.

    The environment variable :envvar:`CELERY_TRACE_APP` is used to
    trace app leaks.  When enabled an exception is raised if there
    is no active app.

    """
    if app is None:
        return getattr(_tls, "current_app", None) or default_app
    return app


def _app_or_default_trace(app=None):  # pragma: no cover
    from traceback import print_stack
    from multiprocessing import current_process
    if app is None:
        if getattr(_tls, "current_app", None):
            print("-- RETURNING TO CURRENT APP --")  # noqa+
            print_stack()
            return _tls.current_app
        if current_process()._name == "MainProcess":
            raise Exception("DEFAULT APP")
        print("-- RETURNING TO DEFAULT APP --")      # noqa+
        print_stack()
        return default_app
    return app

app_or_default = None
def enable_trace():
    global app_or_default
    app_or_default = _app_or_default_trace


def disable_trace():
    global app_or_default
    app_or_default = _app_or_default
