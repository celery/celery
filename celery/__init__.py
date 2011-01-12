"""Distributed Task Queue"""
import os

VERSION = (2, 2, 0, "rc1")

__version__ = ".".join(map(str, VERSION[0:3])) + "".join(VERSION[3:])
__author__ = "Ask Solem"
__contact__ = "ask@celeryproject.org"
__homepage__ = "http://celeryproject.org"
__docformat__ = "restructuredtext"


def Celery(*args, **kwargs):
    from celery.app import App
    return App(*args, **kwargs)


def CompatCelery(*args, **kwargs):
    return Celery(loader=os.environ.get("CELERY_LOADER", "default"))


if not os.environ.get("CELERY_NO_EVAL", False):
    from celery.utils import LocalProxy, instantiate
    current_app = LocalProxy(lambda: instantiate("celery.app.current_app"))
