# -*- coding: utf-8 -*-
"""
    celery.concurrency.base
    ~~~~~~~~~~~~~~~~~~~~~~~

    TaskPool interface.

"""
from __future__ import absolute_import

import logging
import os
import time

from kombu.utils.encoding import safe_repr

from celery.utils import timer2
from celery.utils.log import get_logger

logger = get_logger('celery.concurrency')


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

    #: only used by multiprocessing pool
    uses_semaphore = False

    def __init__(self, limit=None, putlocks=True,
                 forking_enable=True, callbacks_propagate=(), **options):
        self.limit = limit
        self.putlocks = putlocks
        self.options = options
        self.forking_enable = forking_enable
        self.callbacks_propagate = callbacks_propagate
        self._does_debug = logger.isEnabledFor(logging.DEBUG)

    def on_start(self):
        pass

    def did_start_ok(self):
        return True

    def on_stop(self):
        pass

    def on_apply(self, *args, **kwargs):
        pass

    def on_terminate(self):
        pass

    def on_soft_timeout(self, job):
        pass

    def on_hard_timeout(self, job):
        pass

    def maybe_handle_result(self, *args):
        pass

    def maintain_pool(self, *args, **kwargs):
        pass

    def terminate_job(self, pid):
        raise NotImplementedError(
            '%s does not implement kill_job' % (self.__class__, ))

    def restart(self):
        raise NotImplementedError(
            '%s does not implement restart' % (self.__class__, ))

    def stop(self):
        self.on_stop()
        self._state = self.TERMINATE

    def terminate(self):
        self._state = self.TERMINATE
        self.on_terminate()

    def start(self):
        self.on_start()
        self._state = self.RUN

    def close(self):
        self._state = self.CLOSE
        self.on_close()

    def on_close(self):
        pass

    def init_callbacks(self, **kwargs):
        pass

    def apply_async(self, target, args=[], kwargs={}, **options):
        """Equivalent of the :func:`apply` built-in function.

        Callbacks should optimally return as soon as possible since
        otherwise the thread which handles the result will get blocked.

        """
        if self._does_debug:
            logger.debug('TaskPool: Apply %s (args:%s kwargs:%s)',
                         target, safe_repr(args), safe_repr(kwargs))

        return self.on_apply(target, args, kwargs,
                             waitforslot=self.putlocks,
                             callbacks_propagate=self.callbacks_propagate,
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

    @property
    def readers(self):
        return {}

    @property
    def writers(self):
        return {}

    @property
    def timers(self):
        return {}
