# -*- coding: utf-8 -*-
"""
    celery.task.chords
    ~~~~~~~~~~~~~~~~~~

    Chords (task set callbacks).

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from .. import current_app
from ..result import TaskSetResult
from ..utils import uuid

from .sets import TaskSet, subtask


@current_app.task(name="celery.chord_unlock", max_retries=None)
def _unlock_chord(setid, callback, interval=1, propagate=False,
        max_retries=None):
    result = TaskSetResult.restore(setid)
    if result.ready():
        subtask(callback).delay(result.join(propagate=propagate))
        result.delete()
    else:
        _unlock_chord.retry(countdown=interval, max_retries=max_retries)


class Chord(current_app.Task):
    accept_magic_kwargs = False
    name = "celery.chord"

    def run(self, set, body, interval=1, max_retries=None,
            propagate=False, **kwargs):
        if not isinstance(set, TaskSet):
            set = TaskSet(set)
        r = []
        setid = uuid()
        for task in set.tasks:
            tid = uuid()
            task.options.update(task_id=tid, chord=body)
            r.append(current_app.AsyncResult(tid))
        current_app.TaskSetResult(setid, r).save()
        self.backend.on_chord_apply(setid, body, interval,
                                    max_retries=max_retries,
                                    propagate=propagate)
        return set.apply_async(taskset_id=setid)


class chord(object):
    Chord = Chord

    def __init__(self, tasks, **options):
        self.tasks = tasks
        self.options = options

    def __call__(self, body, **options):
        tid = body.options.setdefault("task_id", uuid())
        self.Chord.apply_async((list(self.tasks), body), self.options,
                                **options)
        return body.type.app.AsyncResult(tid)
