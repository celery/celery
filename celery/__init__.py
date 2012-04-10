# -*- coding: utf-8 -*-
"""Distributed Task Queue"""
# :copyright: (c) 2009 - 2012 by Ask Solem.
# :license:   BSD, see LICENSE for more details.

from __future__ import absolute_import

VERSION = (2, 6, 0, "a1")
__version__ = ".".join(map(str, VERSION[0:3])) + "".join(VERSION[3:])
__author__ = "Ask Solem"
__contact__ = "ask@celeryproject.org"
__homepage__ = "http://celeryproject.org"
__docformat__ = "restructuredtext"

# -eof meta-

import sys

if sys.version_info < (2, 5):
    raise Exception(
        "Python 2.4 is not supported by this version. "
        "Please use Celery versions 2.1.x or earlier.")

# Lazy loading
from types import ModuleType
from .local import Proxy
from .__compat__ import create_magic_module


def Celery(*args, **kwargs):
    from .app import App
    return App(*args, **kwargs)


def _get_current_app():
    from .app import current_app
    return current_app()
current_app = Proxy(_get_current_app)


def bugreport():
    return current_app.bugreport()


old_module, new_module = create_magic_module(__name__,
    compat_modules=("messaging", "log", "registry", "decorators"),
    by_module={
        "celery.task.sets": ["chain", "group", "subtask"],
        "celery.task.chords": ["chord"],
    },
    direct={"task": "celery.task"},
    __package__="celery",
    __file__=__file__,
    __path__=__path__,
    __doc__=__doc__,
    __version__=__version__,
    __author__=__author__,
    __contact__=__contact__,
    __homepage__=__homepage__,
    __docformat__=__docformat__,
    VERSION=VERSION,
    Celery=Celery,
    current_app=current_app,
    bugreport=bugreport,
)
