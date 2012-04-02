# -*- coding: utf-8 -*-
"""
    celery.app
    ~~~~~~~~~~

    Celery Application.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import os

from ..local import Proxy

from . import state
from .base import App, AppPickler  # noqa

set_default_app = state.set_default_app
current_app = state.current_app
current_task = state.current_task
default_app = Proxy(lambda: state.default_app)

#: Returns the app provided or the default app if none.
#:
#: The environment variable :envvar:`CELERY_TRACE_APP` is used to
#: trace app leaks.  When enabled an exception is raised if there
#: is no active app.
app_or_default = None

#: The "default" loader is the default loader used by old applications.
default_loader = os.environ.get("CELERY_LOADER") or "default"

#: Global fallback app instance.
set_default_app(App("default", loader=default_loader,
                               set_as_current=False,
                               accept_magic_kwargs=True))


def bugreport():
    return current_app().bugreport()


def _app_or_default(app=None):
    if app is None:
        return current_app()
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
        return state.default_app
    return app

def enable_trace():
    global app_or_default
    app_or_default = _app_or_default_trace


def disable_trace():
    global app_or_default
    app_or_default = _app_or_default

if os.environ.get("CELERY_TRACE_APP"):  # pragma: no cover
    enable_trace()
else:
    disable_trace()
