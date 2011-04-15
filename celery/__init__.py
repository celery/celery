"""Distributed Task Queue"""
# :copyright: (c) 2009 - 2011 by Ask Solem.
# :license:   BSD, see LICENSE for more details.

import os
import sys

VERSION = (2, 3, 0, "a1")

__version__ = ".".join(map(str, VERSION[0:3])) + "".join(VERSION[3:])
__author__ = "Ask Solem"
__contact__ = "ask@celeryproject.org"
__homepage__ = "http://celeryproject.org"
__docformat__ = "restructuredtext"

if sys.version_info < (2, 5):
    raise Exception(
        "Python 2.4 is not supported by this version. "
        "Please use Celery versions 2.1.x or earlier.")


def Celery(*args, **kwargs):
    from celery.app import App
    return App(*args, **kwargs)

if not os.environ.get("CELERY_NO_EVAL", False):
    from celery.local import LocalProxy

    def _get_current_app():
        from celery.app import current_app
        return current_app()

    current_app = LocalProxy(_get_current_app)
