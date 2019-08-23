# -*- coding: utf-8 -*-
"""Old deprecated task module.

This is the old task module, it shouldn't be used anymore,
import from the main 'celery' module instead.
If you're looking for the decorator implementation then that's in
``celery.app.base.Celery.task``.
"""
from __future__ import absolute_import, unicode_literals

from celery._state import current_app
from celery._state import current_task as current
from celery.local import LazyModule, Proxy, recreate_module

__all__ = (
    'BaseTask', 'Task', 'PeriodicTask', 'task', 'periodic_task',
    'group', 'chord', 'subtask',
)


STATICA_HACK = True
globals()['kcah_acitats'[::-1].upper()] = False
if STATICA_HACK:  # pragma: no cover
    # This is never executed, but tricks static analyzers (PyDev, PyCharm,
    # pylint, etc.) into knowing the types of these symbols, and what
    # they contain.
    from celery.canvas import group, chord, subtask
    from .base import BaseTask, Task, PeriodicTask, task, periodic_task


class module(LazyModule):

    def __call__(self, *args, **kwargs):
        return self.task(*args, **kwargs)


old_module, new_module = recreate_module(  # pragma: no cover
    __name__,
    by_module={
        'celery.task.base': ['BaseTask', 'Task', 'PeriodicTask',
                             'task', 'periodic_task'],
        'celery.canvas': ['group', 'chord', 'subtask'],
    },
    base=module,
    __package__='celery.task',
    __file__=__file__,
    __path__=__path__,
    __doc__=__doc__,
    current=current,
    discard_all=Proxy(lambda: current_app.control.purge),
    backend_cleanup=Proxy(
        lambda: current_app.tasks['celery.backend_cleanup']
    ),
)
