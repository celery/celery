import os
import sys
import time
import traceback

from celery import log
from celery.datastructures import ExceptionInfo
from celery.utils.functional import partial
from celery.utils import timer2


def apply_target(target, args=(), kwargs={}, callback=None,
        accept_callback=None, pid=None):
    if accept_callback:
        accept_callback(pid or os.getpid(), time.time())
    callback(target(*args, **kwargs))


class BasePool(object):
    RUN = 0x1
    CLOSE = 0x2
    TERMINATE = 0x3

    Timer = timer2.Timer

    signal_safe = True
    is_green = False

    _state = None
    _pool = None

    def __init__(self, limit=None, putlocks=True, logger=None, **options):
        self.limit = limit
        self.putlocks = putlocks
        self.logger = logger or log.get_default_logger()
        self.options = options

    def on_start(self):
        pass

    def on_stop(self):
        pass

    def on_apply(self, *args, **kwargs):
        pass

    def on_terminate(self):
        pass

    def terminate_job(self, pid):
        raise NotImplementedError(
                "%s does not implement kill_job" % (self.__class__, ))

    def stop(self):
        self._state = self.CLOSE
        self.on_stop()
        self._state = self.TERMINATE

    def terminate(self):
        self._state = self.TERMINATE
        self.on_terminate()

    def start(self):
        self.on_start()
        self._state = self.RUN

    def apply_async(self, target, args=None, kwargs=None, callbacks=None,
            errbacks=None, accept_callback=None, timeout_callback=None,
            **compat):
        """Equivalent of the :func:`apply` built-in function.

        All `callbacks` and `errbacks` should complete immediately since
        otherwise the thread which handles the result will get blocked.

        """
        args = args or []
        kwargs = kwargs or {}
        callbacks = callbacks or []
        errbacks = errbacks or []

        on_ready = partial(self.on_ready, callbacks, errbacks)
        on_worker_error = partial(self.on_worker_error, errbacks)

        self.logger.debug("TaskPool: Apply %s (args:%s kwargs:%s)" % (
            target, args, kwargs))

        return self.on_apply(target, args, kwargs,
                             callback=on_ready,
                             accept_callback=accept_callback,
                             timeout_callback=timeout_callback,
                             error_callback=on_worker_error,
                             waitforslot=self.putlocks)

    def on_ready(self, callbacks, errbacks, ret_value):
        """What to do when a worker task is ready and its return value has
        been collected."""

        if isinstance(ret_value, ExceptionInfo):
            if isinstance(ret_value.exception, (
                    SystemExit, KeyboardInterrupt)):
                raise ret_value.exception
            [self.safe_apply_callback(errback, ret_value)
                    for errback in errbacks]
        else:
            [self.safe_apply_callback(callback, ret_value)
                    for callback in callbacks]

    def on_worker_error(self, errbacks, exc):
        einfo = ExceptionInfo((exc.__class__, exc, None))
        [errback(einfo) for errback in errbacks]

    def safe_apply_callback(self, fun, *args):
        try:
            fun(*args)
        except:
            self.logger.error("Pool callback raised exception: %s" % (
                traceback.format_exc(), ),
                exc_info=sys.exc_info())

    def _get_info(self):
        return {}

    @property
    def info(self):
        return self._get_info()

    @property
    def active(self):
        return self._state == self.RUN
