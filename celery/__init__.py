"""Distributed Task Queue"""
import os
import sys

VERSION = (2, 2, 0, "rc1")

__version__ = ".".join(map(str, VERSION[0:3])) + "".join(VERSION[3:])
__author__ = "Ask Solem"
__contact__ = "ask@celeryproject.org"
__homepage__ = "http://celeryproject.org"
__docformat__ = "restructuredtext"

if sys.version_info < (2, 5):
    import warnings
    warnings.warn(DeprecationWarning("""

Python 2.4 support is deprecated and only versions 2.5, 2.6, 2.7+
will be supported starting from Celery version 2.3.


"""))


def Celery(*args, **kwargs):
    from celery.app import App
    return App(*args, **kwargs)

if not os.environ.get("CELERY_NO_EVAL", False):
    from celery.utils import LocalProxy, instantiate
    current_app = LocalProxy(lambda: instantiate("celery.app.current_app"))
