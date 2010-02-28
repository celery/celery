"""

Process Pools.

"""
from billiard.pool import DynamicPool
from billiard.utils.functional import curry

from celery import log
from celery.datastructures import ExceptionInfo


class TaskPool(object):
    """Process Pool for processing tasks in parallel.

    :param limit: see :attr:`limit`.
    :param logger: see :attr:`logger`.


    .. attribute:: limit

        The number of processes that can run simultaneously.

    .. attribute:: logger

        The logger used for debugging.

    """

    def __init__(self, limit, logger=None, initializer=None):
        self.limit = limit
        self.logger = logger or log.get_default_logger()
        self.initializer = initializer
        self._pool = None

    def start(self):
        """Run the task pool.

        Will pre-fork all workers so they're ready to accept tasks.

        """
        self._pool = DynamicPool(processes=self.limit,
                                 initializer=self.initializer)

    def stop(self):
        """Terminate the pool."""
        self._pool.close()
        self._pool.join()
        self._pool = None

    def replace_dead_workers(self):
        self.logger.debug("TaskPool: Finding dead pool processes...")
        dead_count = self._pool.replace_dead_workers()
        if dead_count: # pragma: no cover
            self.logger.info(
                "TaskPool: Replaced %d dead pool workers..." % (
                    dead_count))

    def apply_async(self, target, args=None, kwargs=None, callbacks=None,
            errbacks=None, **compat):
        """Equivalent of the :func:``apply`` built-in function.

        All ``callbacks`` and ``errbacks`` should complete immediately since
        otherwise the thread which handles the result will get blocked.

        """
        args = args or []
        kwargs = kwargs or {}
        callbacks = callbacks or []
        errbacks = errbacks or []

        on_ready = curry(self.on_ready, callbacks, errbacks)

        self.logger.debug("TaskPool: Apply %s (args:%s kwargs:%s)" % (
            target, args, kwargs))

        self.replace_dead_workers()

        return self._pool.apply_async(target, args, kwargs,
                                        callback=on_ready)

    def on_ready(self, callbacks, errbacks, ret_value):
        """What to do when a worker task is ready and its return value has
        been collected."""

        if isinstance(ret_value, ExceptionInfo):
            if isinstance(ret_value.exception, (
                    SystemExit, KeyboardInterrupt)): # pragma: no cover
                raise ret_value.exception
            [errback(ret_value) for errback in errbacks]
        else:
            [callback(ret_value) for callback in callbacks]
