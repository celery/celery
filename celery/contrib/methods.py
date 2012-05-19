from __future__ import absolute_import

from functools import partial

from celery import task as _task


class task_method(object):

    def __init__(self, task, *args, **kwargs):
        self.task = task

    def __get__(self, obj, type=None):
        if obj is None:
            return self.task
        task = self.task.__class__()
        task.__self__ = obj
        return task


task = partial(_task, filter=task_method)
