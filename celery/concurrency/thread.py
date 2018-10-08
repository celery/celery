# -*- coding: utf-8 -*-
"""Thread execution pool."""
from __future__ import absolute_import, unicode_literals

from concurrent.futures import wait, ThreadPoolExecutor
from .base import BasePool, apply_target

__all__ = ('TaskPool',)


class ApplyResult(object):
    def __init__(self, future):
        self.f = future
        self.get = self.f.result

    def wait(self, timeout=None):
        wait([self.f], timeout)


class TaskPool(BasePool):
    """Thread Task Pool."""

    body_can_be_buffer = True
    signal_safe = False

    def __init__(self, *args, **kwargs):
        super(TaskPool, self).__init__(*args, **kwargs)
        if self.limit is not None:
            self.executor = ThreadPoolExecutor(max_workers=self.limit)
        else:
            self.executor = ThreadPoolExecutor()  # Python < 3.5

    def on_stop(self):
        self.executor.shutdown()
        super(TaskPool, self).on_stop()

    def on_apply(self, target, args=None, kwargs=None, callback=None,
                 accept_callback=None, **_):
        f = self.executor.submit(apply_target, target, args, kwargs,
                                 callback, accept_callback)
        return ApplyResult(f)

    def _get_info(self):
        return {
            'max-concurrency': self.limit,
            'threads': len(self.executor._threads)
            # TODO use a public api to retrieve the current number of threads
            # in the executor when available. (Currently not available).
        }
