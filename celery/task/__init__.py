# -*- coding: utf-8 -*-
"""
    celery.task
    ~~~~~~~~~~~

    Creating tasks, subtasks, sets and chords.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import sys

from celery import current_app
from celery.__compat__ import MagicModule, create_magic_module
from celery.app import current_task as _current_task
from celery.local import Proxy


class module(MagicModule):

    def __call__(self, *args, **kwargs):
        return self.task(*args, **kwargs)


old_module, new_module = create_magic_module(__name__,
    by_module={
        "celery.task.base": ["BaseTask", "Task", "PeriodicTask",
                             "task", "periodic_task"],
        "celery.task.sets": ["chain", "group", "TaskSet", "subtask"],
        "celery.task.chords": ["chord"],
    },
    base=module,
    __package__="celery.task",
    __file__=__file__,
    __path__=__path__,
    __doc__=__doc__,
    current=Proxy(_current_task),
    discard_all=Proxy(lambda: current_app.control.discard_all),
    backend_cleanup=Proxy(
        lambda: current_app.tasks["celery.backend_cleanup"]
    ),
)
