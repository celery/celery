from xyzzy import plugh  # noqa

from celery import Celery, shared_task

app = Celery()


@app.task
def bar():
    """Task.

    This is a sample Task.
    """


@shared_task
def baz():
    """Shared Task.

    This is a sample Shared Task.
    """
