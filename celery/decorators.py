"""

Decorators

"""
from celery import task as _task


def task(*args, **kwargs):
    kwargs.setdefault("accept_magic_kwargs", True)
    return _task.task(*args, **kwargs)


def periodic_task(*args, **kwargs):
    kwargs.setdefault("accept_magic_kwargs", True)
    return _task.periodic_task(*args, **kwargs)


