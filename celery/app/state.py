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


def current_task():
    return getattr(_tls, "current_task", None)
