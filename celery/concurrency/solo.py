# -*- coding: utf-8 -*-
"""Single-threaded execution pool."""
<<<<<<< HEAD
from __future__ import absolute_import, unicode_literals

=======
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
import os

from .base import BasePool, apply_target

__all__ = ('TaskPool',)


class TaskPool(BasePool):
    """Solo task pool (blocking, inline, fast)."""

    body_can_be_buffer = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.on_apply = apply_target
        self.limit = 1

    def _get_info(self):
        return {
            'max-concurrency': 1,
            'processes': [os.getpid()],
            'max-tasks-per-child': None,
            'put-guarded-by-semaphore': True,
            'timeouts': (),
        }
