# -*- coding: utf-8 -*-
"""
    celery.worker.mediator
    ~~~~~~~~~~~~~~~~~~~~~~

    The mediator is an internal thread that moves tasks
    from an internal :class:`Queue` to the worker pool.

    This is only used if rate limits are enabled, as it moves
    messages from the rate limited queue (which holds tasks
    that are allowed to be processed) to the pool. Disabling
    rate limits will also disable this machinery,
    and can improve performance.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import traceback

from Queue import Empty

from ..app import app_or_default
from ..utils.threads import bgThread


class Mediator(bgThread):

    #: The task queue, a :class:`~Queue.Queue` instance.
    ready_queue = None

    #: Callback called when a task is obtained.
    callback = None

    def __init__(self, ready_queue, callback, logger=None, app=None):
        self.app = app_or_default(app)
        self.logger = logger or self.app.log.get_default_logger()
        self.ready_queue = ready_queue
        self.callback = callback
        super(Mediator, self).__init__()

    def next(self):
        try:
            task = self.ready_queue.get(timeout=1.0)
        except Empty:
            return

        if task.revoked():
            return

        self.logger.debug(
            "Mediator: Running callback for task: %s[%s]" % (
                task.task_name, task.task_id))

        try:
            self.callback(task)
        except Exception, exc:
            self.logger.error("Mediator callback raised exception %r\n%s",
                              exc, traceback.format_exc(),
                              exc_info=sys.exc_info(),
                              extra={"data": {"id": task.task_id,
                                              "name": task.task_name,
                                              "hostname": task.hostname}})
    move = next   # XXX compat
