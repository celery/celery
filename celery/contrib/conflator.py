# -*- coding: utf-8 -*-
"""celery.contrib.conflator
======================

.. versionadded :: 3.2.0

A task which will check that it's enqueued time is after the last time
a task with the same conflation ID was executed to clear backed up
work that's already been done.

This is useful for e.g. periodic tasks which occasionally take longer
than their period to execute such as checking in with a remote system
which may be down, experiencing performance issues or so on.

It's important that things should work out satisfactorily at task
execution time if all previously scheduled tasks execute or if they
have all been conflated into 1.

**Simple Example**

.. code-block:: python

    # sync from a remote system
    @app.periodic_task(base=Conflator, run_every=timedelta(minutes=1))
    def update_from_remote():
        from remote_system import update
        update()

"""

from datetime import datetime

from celery import states, Task
from celery.utils.log import get_logger

logger = get_logger(__name__)

class Conflator(Task):

    abstract = True

    conflation_key = None

    def apply_async(self, *args, **kwargs):
        conflation_key = self.conflation_key
        if conflation_key is None:
            conflation_key = kwargs.get('conflation_key', 
                                        self.__class__.__name__)
        pending = self.backend.get_task_meta(conflation_key)

        # If there's a pending task just skip it
        if not pending['result']:
            self.backend.store_result(conflation_key, True, 
                                      "conflating")
            return super(Conflator, self).apply_async(args, kwargs)
        else:
            logger.info("Conflating task with key %s", conflation_key)

    def __call__(self, *args, **kwargs):

        conflation_key = self.conflation_key
        if conflation_key is None:
            conflation_key = self.__class__.__name__

        # always clear the conflation key so tasks that error out
        # don't block the queue
        self.backend.forget(conflation_key)
        return super(Conflator, self).__call__(*args, **kwargs)
