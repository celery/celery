# -*- coding: utf-8 -*-
"""Distributed Task Queue"""
# :copyright: (c) 2009 - 2012 by Ask Solem.
# :license:   BSD, see LICENSE for more details.

from __future__ import absolute_import

VERSION = (2, 5, 3)
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

from .local import Proxy


def Celery(*args, **kwargs):
    from .app import App
    return App(*args, **kwargs)


def _get_current_app():
    from .app import current_app
    return current_app()
current_app = Proxy(_get_current_app)
