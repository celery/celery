# -*- coding: utf-8 -*-
from __future__ import absolute_import

from celery.utils import uuid

#: global list of functions defining a built-in task.
#: these are called for every app instance to setup built-in task.
_builtin_tasks = []


def builtin_task(constructor):
    """Decorator that specifies that the decorated function is a function
    that generates a built-in task.

    The function will then be called for every new app instance created
    (lazily, so more exactly when the task registry for that app is needed).
    """
    _builtin_tasks.append(constructor)
    return constructor


def load_builtin_tasks(app):
    """Loads the built-in tasks for an app instance."""
    [constructor(app) for constructor in _builtin_tasks]


@builtin_task
def add_backend_cleanup_task(app):
    """The backend cleanup task can be used to clean up the default result
    backend.

    This task is also added do the periodic task schedule so that it is
    run every day at midnight, but :program:`celerybeat` must be running
    for this to be effective.

    Note that not all backends do anything for this, what needs to be
    done at cleanup is up to each backend, and some backends
    may even clean up in realtime so that a periodic cleanup is not necessary.

    """
    return app.task(name="celery.backend_cleanup")(app.backend.cleanup)


@builtin_task
def add_unlock_chord_task(app):
    """The unlock chord task is used by result backends that doesn't
    have native chord support.

    It creates a task chain polling the header for completion.

    """
    from celery.task.sets import subtask
    from celery import result as _res

    @app.task(name="celery.chord_unlock", max_retries=None)
    def unlock_chord(setid, callback, interval=1, propagate=False,
            max_retries=None, result=None):
        result = _res.TaskSetResult(setid, map(_res.AsyncResult, result))
        j = result.join_native if result.supports_native_join else result.join
        if result.ready():
            subtask(callback).delay(j(propagate=propagate))
        else:
            unlock_chord.retry(countdown=interval, max_retries=max_retries)

    return unlock_chord


@builtin_task
def add_chord_task(app):
    """Every chord is executed in a dedicated task, so that the chord
    can be used as a subtask, and this generates the task
    responsible for that."""
    from celery.task.sets import TaskSet

    @app.task(name="celery.chord", accept_magic_kwargs=False)
    def chord(set, body, interval=1, max_retries=None,
            propagate=False, **kwargs):

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
        set.apply_async(taskset_id=setid)

    return chord
