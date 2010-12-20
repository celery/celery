"""Distributed Task Queue"""
import os

VERSION = (2, 2, 0, "b1")

__version__ = ".".join(map(str, VERSION[0:3])) + "".join(VERSION[3:])
__author__ = "Ask Solem"
__contact__ = "ask@celeryproject.org"
__homepage__ = "http://celeryproject.org"
__docformat__ = "restructuredtext"


def Celery(*args, **kwargs):
    from celery import app
    return app.App(*args, **kwargs)


def CompatCelery(*args, **kwargs):
    return Celery(loader=os.environ.get("CELERY_LOADER", "default"))
