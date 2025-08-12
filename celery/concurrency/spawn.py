"""Spawn execution pool."""
import os

import billiard

from .prefork import TaskPool as PreforkTaskPool

__all__ = ("TaskPool",)


class TaskPool(PreforkTaskPool):
    """Multiprocessing Pool using the ``spawn`` start method."""

    start_method = "spawn"

    def on_start(self):
        billiard.set_start_method(self.start_method, force=True)
        os.environ.setdefault("FORKED_BY_MULTIPROCESSING", "1")
        super().on_start()
