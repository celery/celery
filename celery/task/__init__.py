# -*- coding: utf-8 -*-
"""
    celery.task
    ~~~~~~~~~~~

    This is the old task module, it should not be used anymore,
    import from the main 'celery' module instead.
    If you're looking for the decorator implementation then that's in
    ``celery.app.base.Celery.task``.

"""
from __future__ import absolute_import

from celery._state import current_app, current_task as current
from celery.__compat__ import MagicModule, recreate_module
from celery.local import Proxy

__all__ = [
    'BaseTask', 'Task', 'PeriodicTask',
    'task', 'periodic_task',
    'group', 'chord', 'subtask', 'TaskSet',
]

# This is for static analyzers
BaseTask = object
Task = object
PeriodicTask = object
task = lambda *a, **kw: None
periodic_task = lambda *a, **kw: None
group = lambda *a, **kw: None
chord = lambda *a, **kw: None
subtask = lambda *a, **kw: None
TaskSet = object


class module(MagicModule):

    def __call__(self, *args, **kwargs):
        return self.task(*args, **kwargs)


old_module, new_module = recreate_module(__name__,  # pragma: no cover
    by_module={
        'celery.task.base':   ['BaseTask', 'Task', 'PeriodicTask',
                               'task', 'periodic_task'],
        'celery.canvas':      ['chain', 'group', 'chord', 'subtask'],
        'celery.task.sets':   ['TaskSet'],
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
