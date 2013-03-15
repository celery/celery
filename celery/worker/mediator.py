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

"""
from __future__ import absolute_import

import logging

from Queue import Empty

from celery.app import app_or_default
from celery.utils.threads import bgThread
from celery.utils.log import get_logger

from .bootsteps import StartStopComponent

logger = get_logger(__name__)


class WorkerComponent(StartStopComponent):
    name = 'worker.mediator'
    requires = ('pool', 'queues', )

    def __init__(self, w, **kwargs):
        w.mediator = None

    def include_if(self, w):
        return w.start_mediator and not w.use_eventloop

    def create(self, w):
        m = w.mediator = self.instantiate(w.mediator_cls, w.ready_queue,
                                          app=w.app, callback=w.process_task)
        return m


class Mediator(bgThread):
    """Mediator thread."""

    #: The task queue, a :class:`~Queue.Queue` instance.
    ready_queue = None

    #: Callback called when a task is obtained.
    callback = None

    def __init__(self, ready_queue, callback, app=None, **kw):
        self.app = app_or_default(app)
        self.ready_queue = ready_queue
        self.callback = callback
        self._does_debug = logger.isEnabledFor(logging.DEBUG)
        super(Mediator, self).__init__()

    def body(self):
        try:
            task = self.ready_queue.get(timeout=1.0)
        except Empty:
            return

        if self._does_debug:
            logger.debug('Mediator: Running callback for task: %s[%s]',
                         task.name, task.id)

        try:
            self.callback(task)
        except Exception, exc:
            logger.error('Mediator callback raised exception %r',
                         exc, exc_info=True,
                         extra={'data': {'id': task.id,
                                         'name': task.name,
                                         'hostname': task.hostname}})
