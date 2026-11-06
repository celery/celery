"""Single-threaded execution pool."""
import os
from functools import partial

from celery import signals
from celery.utils.log import get_logger

from .base import BasePool, apply_target

__all__ = ('TaskPool',)

logger = get_logger(__name__)


def _safe_callback(callback, propagate, *args, **kwargs):
    # The solo pool runs the task's callback in the consumer, so an error
    # from it, such as a result backend that is down, would reach the
    # consumer, which can take it for a lost broker connection and
    # restart.  Log it instead, as billiard does for the prefork pool.
    try:
        return callback(*args, **kwargs)
    except MemoryError:
        raise
    except propagate:
        raise
    except Exception as exc:  # pylint: disable=broad-except
        logger.error('Pool callback raised exception: %r', exc,
                     exc_info=True)


class TaskPool(BasePool):
    """Solo task pool (blocking, inline, fast)."""

    body_can_be_buffer = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.limit = 1
        # The worker passes (app, hostname) as initargs to every pool.
        initargs = self.options.get('initargs') or (None, None)
        signals.worker_process_init.send(sender=initargs[1])

    def on_apply(self, target, args=(), kwargs=None, callback=None,
                 accept_callback=None, callbacks_propagate=(), **options):
        if callback is not None:
            callback = partial(_safe_callback, callback,
                               tuple(callbacks_propagate))
        return apply_target(target, args, kwargs, callback,
                            accept_callback, **options)

    def _get_info(self):
        info = super()._get_info()
        info.update({
            'max-concurrency': 1,
            'processes': [os.getpid()],
            'max-tasks-per-child': None,
            'put-guarded-by-semaphore': True,
            'timeouts': (),
        })
        return info
