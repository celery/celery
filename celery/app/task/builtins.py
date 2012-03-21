# -*- coding: utf-8 -*-
from __future__ import absolute_import

from ...utils import uuid

_builtins = []


def builtin_task(constructor):
    _builtins.append(constructor)
    return constructor


@builtin_task
def add_backend_cleanup_task(app):

    @app.task(name="celery.backend_cleanup")
    def backend_cleanup():
        app.backend.cleanup()

    return backend_cleanup


@builtin_task
def add_unlock_chord_task(app):

    @app.task(name="celery.chord_unlock", max_retries=None)
    def unlock_chord(setid, callback, interval=1, propagate=False,
            max_retries=None, result=None):
        from ...result import AsyncResult, TaskSetResult
        from ...task.sets import subtask

        result = TaskSetResult(setid, map(AsyncResult, result))
        j = result.join_native if result.supports_native_join else result.join
        if result.ready():
            subtask(callback).delay(j(propagate=propagate))
        else:
            unlock_chord.retry(countdown=interval, max_retries=max_retries)

    return unlock_chord


@builtin_task
def add_chord_task(app):

    @app.task(name="celery.chord", accept_magic_kwargs=False)
    def chord(set, body, interval=1, max_retries=None,
            propagate=False, **kwargs):
        from ...task.sets import TaskSet

        if not isinstance(set, TaskSet):
            set = TaskSet(set)
        r = []
        setid = uuid()
        for task in set.tasks:
            tid = uuid()
            task.options.update(task_id=tid, chord=body)
            r.append(app.AsyncResult(tid))
        app.backend.on_chord_apply(setid, body,
                                   interval=interval,
                                   max_retries=max_retries,
                                   propagate=propagate,
                                   result=r)
        return set.apply_async(taskset_id=setid)

    return chord

def load_builtins(app):
    for constructor in _builtins:
        constructor(app)
