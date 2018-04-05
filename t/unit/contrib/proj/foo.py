from __future__ import absolute_import, unicode_literals

from celery import Celery
from xyzzy import plugh  # noqa

app = Celery()


@app.task
def bar():
    """This task has a docstring!"""
