"""Base Execution Pool."""
import logging
import os
import sys
import time
from threading import Event, Thread
from typing import Any, Callable, Dict

from billiard.einfo import ExceptionInfo
from billiard.exceptions import WorkerLostError
from kombu.asynchronous import get_event_loop
from kombu.utils.encoding import safe_repr

from celery.exceptions import WorkerShutdown, WorkerTerminate, reraise
from celery.utils import timer2
from celery.utils.log import get_logger
from celery.utils.text import truncate

__all__ = ('AsyncPoolShutdownMixin', 'BasePool', 'apply_target')

logger = get_logger('celery.pool')


def apply_target(target, args=(), kwargs=None, callback=None,
                 accept_callback=None, pid=None, getpid=os.getpid,
                 propagate=(), monotonic=time.monotonic, **_):
    """Apply function within pool context."""
    kwargs = {} if not kwargs else kwargs
    if accept_callback:
        accept_callback(pid or getpid(), monotonic())
    try:
        ret = target(*args, **kwargs)
    except propagate:
        raise
    except Exception:
        raise
    except (WorkerShutdown, WorkerTerminate):
        raise
    except BaseException as exc:
        try:
            reraise(WorkerLostError, WorkerLostError(repr(exc)),
                    sys.exc_info()[2])
        except WorkerLostError:
            callback(ExceptionInfo())
    else:
        callback(ret)


class BasePool:
    """Task pool."""

    RUN = 0x1
    CLOSE = 0x2
    TERMINATE = 0x3

    Timer = timer2.Timer

    #: set to true if the pool can be shutdown from within
    #: a signal handler.
    signal_safe = True

    #: set to true if pool uses greenlets.
    is_green = False

    _state = None
    _pool = None
    _does_debug = True

    #: only used by multiprocessing pool
    uses_semaphore = False

    task_join_will_block = True
    body_can_be_buffer = False

    def __init__(self, limit=None, putlocks=True, forking_enable=True,
                 callbacks_propagate=(), app=None, **options):
        self.limit = limit
        self.putlocks = putlocks
        self.options = options
        self.forking_enable = forking_enable
        self.callbacks_propagate = callbacks_propagate
        self.app = app

    def on_start(self):
        pass

    def did_start_ok(self):
        return True

    def flush(self):
        pass

    def on_stop(self):
        pass

    def register_with_event_loop(self, loop):
        pass

    def on_apply(self, *args, **kwargs):
        pass

    def on_terminate(self):
        pass

    def on_soft_timeout(self, job):
        pass

    def on_hard_timeout(self, job):
        pass

    def maintain_pool(self, *args, **kwargs):
        pass

    def terminate_job(self, pid, signal=None):
        raise NotImplementedError(
            f'{type(self)} does not implement kill_job')

    def restart(self):
        raise NotImplementedError(
            f'{type(self)} does not implement restart')

    def stop(self):
        self.on_stop()
        self._state = self.TERMINATE

    def terminate(self):
        self._state = self.TERMINATE
        self.on_terminate()

    def start(self):
        self._does_debug = logger.isEnabledFor(logging.DEBUG)
        self.on_start()
        self._state = self.RUN

    def close(self):
        self._state = self.CLOSE
        self.on_close()

    def on_close(self):
        pass

    def apply_async(self, target, args=None, kwargs=None, **options):
        """Equivalent of the :func:`apply` built-in function.

        Callbacks should optimally return as soon as possible since
        otherwise the thread which handles the result will get blocked.
        """
        kwargs = {} if not kwargs else kwargs
        args = [] if not args else args
        if self._does_debug:
            logger.debug('TaskPool: Apply %s (args:%s kwargs:%s)',
                         target, truncate(safe_repr(args), 1024),
                         truncate(safe_repr(kwargs), 1024))

        return self.on_apply(target, args, kwargs,
                             waitforslot=self.putlocks,
                             callbacks_propagate=self.callbacks_propagate,
                             **options)

    def _get_info(self) -> Dict[str, Any]:
        """
        Return configuration and statistics information. Subclasses should
        augment the data as required.

        :return: The returned value must be JSON-friendly.
        """
        return {
            'implementation': self.__class__.__module__ + ':' + self.__class__.__name__,
            'max-concurrency': self.limit,
        }

    @property
    def info(self):
        return self._get_info()

    @property
    def active(self):
        return self._state == self.RUN

    @property
    def num_processes(self):
        return self.limit


class AsyncPoolShutdownMixin:
    """
    Provides a utility method for async pool (e.g. prefork, threads) shutdown.

    These pools must start their own timer thread to continue firing timer events
    during pool shutdown. This allows heartbeats to continue to be sent while
    long-running tasks drain from the pool.
    """

    @staticmethod
    def start_timer_event_loop(*, pool_type: str) -> tuple[Event, Thread] | None:
        # Keep firing timers (for heartbeats on async transports) while
        # the pool drains. If not using an async transport, no hub exists
        # and the timer thread is not created.
        hub = get_event_loop()
        if hub is not None:
            shutdown_event = Event()

            def fire_timers_loop():
                while not shutdown_event.is_set():
                    try:
                        hub.fire_timers()
                    except Exception:
                        logger.warning(
                            f"Exception in timer thread during {pool_type} on_stop()",
                            exc_info=True,
                        )
                    # 0.5 seconds was chosen as a balance between joining quickly
                    # after the thread/pool join is complete and sleeping long enough to
                    # avoid excessive CPU usage.
                    time.sleep(0.5)

            timer_thread = Thread(
                target=fire_timers_loop,
                daemon=True,
                name=f"{pool_type}-timer-shutdown",
            )
            timer_thread.start()

            return shutdown_event, timer_thread

    @classmethod
    def shutdown_with_timer_loop(cls, *, pool_type: str, shutdown_function: Callable):
        if event_loop_started := cls.start_timer_event_loop(pool_type=pool_type):
            shutdown_event, timer_thread = event_loop_started

            try:
                shutdown_function()
            finally:
                shutdown_event.set()
                timer_thread.join(timeout=1.0)

                if timer_thread.is_alive():
                    logger.warning(
                        f"Timer thread in {pool_type} on_stop() did not terminate cleanly"
                    )
        else:
            shutdown_function()
