
import threading
from threadpool import ThreadPool, WorkRequest

from celery import log
from celery.utils.functional import curry
from celery.datastructures import ExceptionInfo


accept_lock = threading.Lock()


def do_work(target, args=(), kwargs={}, callback=None,
        accept_callback=None):
    accept_lock.acquire()
    try:
        accept_callback()
    finally:
        accept_lock.release()
    callback(target(*args, **kwargs))


class TaskPool(object):

    def __init__(self, limit, logger=None, **kwargs):
        self.limit = limit
        self.logger = logger or log.get_default_logger()
        self._pool = None

    def start(self):
        self._pool = ThreadPool(self.limit)

    def stop(self):
        self._pool.dismissWorkers(self.limit, do_join=True)

    def apply_async(self, target, args=None, kwargs=None, callbacks=None,
            errbacks=None, accept_callback=None, **compat):
        args = args or []
        kwargs = kwargs or {}
        callbacks = callbacks or []
        errbacks = errbacks or []

        on_ready = curry(self.on_ready, callbacks, errbacks)

        self.logger.debug("ThreadPool: Apply %s (args:%s kwargs:%s)" % (
            target, args, kwargs))

        req = WorkRequest(do_work, (target, args, kwargs, on_ready,
                                    accept_callback))
        self._pool.putRequest(req)
        # threadpool also has callback support,
        # but for some reason the callback is not triggered
        # before you've collected the results.
        # Clear the results (if any), so it doesn't grow too large.
        self._pool._results_queue.queue.clear()
        return req

    def on_ready(self, callbacks, errbacks, ret_value):
        """What to do when a worker task is ready and its return value has
        been collected."""

        if isinstance(ret_value, ExceptionInfo):
            if isinstance(ret_value.exception, (
                    SystemExit, KeyboardInterrupt)):        # pragma: no cover
                raise ret_value.exception
            [errback(ret_value) for errback in errbacks]
        else:
            [callback(ret_value) for callback in callbacks]
