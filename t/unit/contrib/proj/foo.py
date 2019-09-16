from __future__ import absolute_import, unicode_literals

from celery import Celery, shared_task
from xyzzy import plugh  # noqa

app = Celery()


@app.task
def bar():
    """Task.

    This is a sample Task.
    """
    pass


@shared_task
def baz():
    """Shared Task.

    This is a sample Shared Task.
    """
    pass
