# copy from https://github.com/intuited/terminable_thread
"""Code based on [http://sebulba.wikispaces.com/recipe+thread2].
`terminable_thread` provides a subclass of `threading.Thread`,
  adding the facility to raise exceptions in the context of the given thread.
See the `README` for information on problematic issues, licensing,
  and other shenanigans.
    >>> import time
    >>>
    >>> def f():
    ...     try:
    ...         while True:
    ...             time.sleep(0.1)
    ...     finally:
    ...         print "outta here"
    ...
    >>> t = Thread(target = f)
    >>> t.start()
    >>> t.isAlive()
    True
    >>> t.terminate()
    >>> t.join()
    outta here
    >>> t.isAlive()
    False
"""
# This program is free software.
# It comes without any warranty, to the extent permitted by applicable law.
# You can redistribute it and/or modify it
#   under the terms of the Do What The Fuck You Want To Public License, Version 2,
#   as published by Sam Hocevar.
# See http://sam.zoy.org/wtfpl/COPYING for more details.
# more inforemathion http://tomerfiliba.com/recipes/Thread2/

import threading
import inspect
import ctypes
from unittest import mock
from concurrent.futures import ThreadPoolExecutor as _T

from celery.exceptions import WorkerTerminate


def _async_raise(tid, exctype):
    """raises the exception, performs cleanup if needed"""
    if not inspect.isclass(exctype):
        raise TypeError("Only types can be raised (not instances)")
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid),
                                                     ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        # """if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"""
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, 0)
        raise SystemError("PyThreadState_SetAsyncExc failed")


class Thread(threading.Thread):
    def _get_my_tid(self):
        """determines this (self's) thread id"""
        if not self.isAlive():
            raise threading.ThreadError("the thread is not active")

        # do we have it cached?
        if hasattr(self, "_thread_id"):
            return self._thread_id

        # no, look for it in the _active dict
        for tid, tobj in threading._active.items():
            if tobj is self:
                self._thread_id = tid
                return tid

        raise AssertionError("could not determine the thread's id")

    def raise_exc(self, exctype):
        """raises the given exception type in the context of this thread"""
        _async_raise(self._get_my_tid(), exctype)

    def terminate(self):
        """raises SystemExit in the context of the given thread, which should 
        cause the thread to exit silently (unless caught)"""
        self.raise_exc(WorkerTerminate)


class ThreadPoolExecutor(_T):
    """
    1. Don't catch WorkerTerminate exception, unless you know what you do
    2. The exception will be raised only when execution returns to the python code.
    """
    def _adjust_thread_count(self):
        with mock.patch("threading.Thread", new=Thread):
            super()._adjust_thread_count()
