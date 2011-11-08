# -*- coding: utf-8 -*-
from __future__ import absolute_import

import os

from .base import BasePool, apply_target


class TaskPool(BasePool):
    """Solo task pool (blocking, inline)."""

    def on_start(self):
        self.pid = os.getpid()

    def on_apply(self, target, args, kwargs, callback=None,
            accept_callback=None, **_):
        return apply_target(target, args, kwargs,
                            callback, accept_callback, self.pid)

    def _get_info(self):
        return {"max-concurrency": 1,
                "processes": [self.pid],
                "max-tasks-per-child": None,
                "put-guarded-by-semaphore": True,
                "timeouts": ()}
