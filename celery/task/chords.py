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
from ..utils import uuid
from ..task.sets import subtask

class chord(object):
    Chord = None

    def __init__(self, tasks, **options):
        self.tasks = tasks
        self.options = options
        self.Chord = self.Chord or current_app.tasks["celery.chord"]

    def __call__(self, body, **options):
        tid = body.options.setdefault("task_id", uuid())
        taskset_result = self.Chord.apply_async((list(self.tasks), body), self.options,
                                **options)
        if self.Chord.app.conf.CELERY_ALWAYS_EAGER:
            return subtask(body).apply(args=(taskset_result.result.join(),))
            
        return body.type.AsyncResult(tid)
