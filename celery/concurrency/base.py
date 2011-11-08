# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging
import os
import sys
import time
import traceback

from functools import partial

from .. import log
from ..datastructures import ExceptionInfo
from ..utils import timer2
from ..utils.encoding import safe_repr


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
    rlimit_safe = True
    is_green = False

    _state = None
    _pool = None

    def __init__(self, limit=None, putlocks=True, logger=None, **options):
        self.limit = limit
        self.putlocks = putlocks
        self.logger = logger or log.get_default_logger()
        self.options = options
        self.does_debug = self.logger.isEnabledFor(logging.DEBUG)

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

    def apply_async(self, target, args=None, kwargs=None, callback=None,
            errback=None, accept_callback=None, timeout_callback=None,
            soft_timeout=None, timeout=None, **compat):
        """Equivalent of the :func:`apply` built-in function.

        Callbacks should optimally return as soon as possible ince
        otherwise the thread which handles the result will get blocked.

        """
        args = args or []
        kwargs = kwargs or {}

        on_ready = partial(self.on_ready, callback, errback)
        on_worker_error = partial(self.on_worker_error, errback)

        if self.does_debug:
            self.logger.debug("TaskPool: Apply %s (args:%s kwargs:%s)",
                            target, safe_repr(args), safe_repr(kwargs))

        return self.on_apply(target, args, kwargs,
                             callback=on_ready,
                             accept_callback=accept_callback,
                             timeout_callback=timeout_callback,
                             error_callback=on_worker_error,
                             waitforslot=self.putlocks,
                             soft_timeout=soft_timeout,
                             timeout=timeout)

    def on_ready(self, callback, errback, ret_value):
        """What to do when a worker task is ready and its return value has
        been collected."""

        if isinstance(ret_value, ExceptionInfo):
            if isinstance(ret_value.exception, (
                    SystemExit, KeyboardInterrupt)):
                raise ret_value.exception
            self.safe_apply_callback(errback, ret_value)
        else:
            self.safe_apply_callback(callback, ret_value)

    def on_worker_error(self, errback, exc_info):
        errback(exc_info)

    def safe_apply_callback(self, fun, *args):
        if fun:
            try:
                fun(*args)
            except BaseException:
                self.logger.error("Pool callback raised exception: %s",
                                  traceback.format_exc(),
                                  exc_info=sys.exc_info())

    def _get_info(self):
        return {}

    @property
    def info(self):
        return self._get_info()

    @property
    def active(self):
        return self._state == self.RUN

    @property
    def num_processes(self):
        return self.limit
