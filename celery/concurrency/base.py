# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging
import os
import time

from .. import log
from ..utils import timer2
from ..utils.encoding import safe_repr


def apply_target(target, args=(), kwargs={}, callback=None,
        accept_callback=None, pid=None, **_):
    if accept_callback:
        accept_callback(pid or os.getpid(), time.time())
    callback(target(*args, **kwargs))


class BasePool(object):
    RUN = 0x1
    CLOSE = 0x2
    TERMINATE = 0x3

    Timer = timer2.Timer

    #: set to true if the pool can be shutdown from within
    #: a signal handler.
    signal_safe = True

    #: set to true if pool supports rate limits.
    #: (this is here for gevent, which currently does not implement
    #: the necessary timers).
    rlimit_safe = True

    #: set to true if pool requires the use of a mediator
    #: thread (e.g. if applying new items can block the current thread).
    requires_mediator = False

    #: set to true if pool uses greenlets.
    is_green = False

    _state = None
    _pool = None

    def __init__(self, limit=None, putlocks=True, logger=None, **options):
        self.limit = limit
        self.putlocks = putlocks
        self.logger = logger or log.get_default_logger()
        self.options = options
        self._does_debug = self.logger.isEnabledFor(logging.DEBUG)

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

    def restart(self):
        raise NotImplementedError(
                "%s does not implement restart" % (self.__class__, ))

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

    def apply_async(self, target, args=[], kwargs={}, **options):
        """Equivalent of the :func:`apply` built-in function.

        Callbacks should optimally return as soon as possible since
        otherwise the thread which handles the result will get blocked.

        """
        if self._does_debug:
            self.logger.debug("TaskPool: Apply %s (args:%s kwargs:%s)",
                            target, safe_repr(args), safe_repr(kwargs))

        return self.on_apply(target, args, kwargs,
                             waitforslot=self.putlocks,
                             **options)

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
