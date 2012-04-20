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

from celery.local import Proxy

from . import state
from .state import (  # noqa
        set_default_app,
        get_current_app as current_app,
        get_current_task as current_task,
)
from .base import Celery, AppPickler  # noqa

#: Proxy always returning the app set as default.
default_app = Proxy(lambda: state.default_app)

#: Function returning the app provided or the default app if none.
#:
#: The environment variable :envvar:`CELERY_TRACE_APP` is used to
#: trace app leaks.  When enabled an exception is raised if there
#: is no active app.
app_or_default = None

#: The "default" loader is the default loader used by old applications.
default_loader = os.environ.get("CELERY_LOADER") or "default"

#: Global fallback app instance.
set_default_app(Celery("default", loader=default_loader,
                                  set_as_current=False,
                                  accept_magic_kwargs=True))


def bugreport():
    return current_app.bugreport()


def _app_or_default(app=None):
    if app is None:
        return state.get_current_app()
    return app


def _app_or_default_trace(app=None):  # pragma: no cover
    from traceback import print_stack
    from celery.utils.mp import get_process_name
    if app is None:
        if getattr(state._tls, "current_app", None):
            print("-- RETURNING TO CURRENT APP --")  # noqa+
            print_stack()
            return state._tls.current_app
        if get_process_name() == "MainProcess":
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

App = Celery  # XXX Compat
