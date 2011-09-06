# -*- coding: utf-8 -*-
from __future__ import absolute_import

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
        if result.ready():
            j = result.join_native if result.supports_native_join else result.join
            subtask(callback).delay(j(propagate=propagate))
        else:
            unlock_chord.retry(countdown=interval, max_retries=max_retries)

    return unlock_chord


def load_builtins(app):
    for constructor in _builtins:
        constructor(app)


