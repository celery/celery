# -*- coding: utf-8 -*-
"""
    celery.task
    ~~~~~~~~~~~

    This is the old task module, it should not be used anymore.

"""
from __future__ import absolute_import

from celery.state import current_app, current_task as current
from celery.__compat__ import MagicModule, recreate_module
from celery.local import Proxy


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
