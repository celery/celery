# -*- coding: utf-8 -*-
"""
    celery.task.chords
    ~~~~~~~~~~~~~~~~~~

    Chords (task set callbacks).

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from .. import current_app
from ..result import AsyncResult, TaskSetResult
from ..utils import uuid

from .sets import TaskSet, subtask


@current_app.task(name="celery.chord_unlock", max_retries=None)
def _unlock_chord(setid, callback, interval=1, propagate=False,
        max_retries=None, result=None):
    result = TaskSetResult(setid, map(AsyncResult, result))
    if result.ready():
        j = result.join_native if result.supports_native_join else result.join
        subtask(callback).delay(j(propagate=propagate))
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
        self.backend.on_chord_apply(setid, body,
                                    interval=interval,
                                    max_retries=max_retries,
                                    propagate=propagate,
                                    result=r)
        return set.apply_async(taskset_id=setid)


class chord(object):
    Chord = Chord

    def __init__(self, tasks, **options):
        self.tasks = tasks
        self.options = options

    def __call__(self, body, **options):
        tid = body.options.setdefault("task_id", uuid())
        result = self.Chord.apply_async((list(self.tasks), body),
                                        self.options, **options)

        if self.Chord.app.conf.CELERY_ALWAYS_EAGER:
            return subtask(body).apply(args=(result.result.join(),))
        return body.type.AsyncResult(tid)
