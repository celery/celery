# -*- coding: utf-8 -*-
"""
    celery.app.control
    ~~~~~~~~~~~~~~~~~~~

    Client for worker remote control commands.
    Server implementation is in :mod:`celery.worker.control`.

"""
from __future__ import absolute_import

from celery.local import Proxy
from celery._state import (
    connect_on_app_finalize, _get_active_apps, current_app
)

from .base import Control, ControlTask
from .inspect import Inspect, flatten_reply

__all__ = ['Control', 'ControlTask', 'Inspect',
           'flatten_reply', 'shared_controller']


def shared_controller(*args, **kwargs):
    """Create shared remote control tasks (decorator), like
    `shared_task`."""

    def create_shared_controller(**options):

        def __inner(fun):
            name = options.get('name')
            if name and not name.startswith('celery.'):
                name = 'celery.' + name
            else:
                name = 'celery.' + fun.__name__
            options['name'] = name
            # Set as shared task so that unfinalized apps,
            # and future apps will load the task.
            connect_on_app_finalize(
                lambda app: app._task_from_fun(fun, **options)
            )

            # Force all finalized apps to take this task as well.
            for app in _get_active_apps():
                if app.finalized:
                    with app._finalize_mutex:
                        app._task_from_fun(fun, **options)

            # Return a proxy that always gets the task from the current
            # apps task registry.
            def task_by_cons():
                app = current_app()
                return app.tasks[name]
            return Proxy(task_by_cons)
        return __inner

    if len(args) == 1 and callable(args[0]):
        return create_shared_controller(**kwargs)(args[0])
    return create_shared_controller(*args, **kwargs)
