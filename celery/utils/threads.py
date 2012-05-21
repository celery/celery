from __future__ import absolute_import

import os
import sys
import threading
import traceback

_Thread = threading.Thread
_Event = threading._Event

active_count = (getattr(threading, "active_count", None) or
                threading.activeCount)


class Event(_Event):

    if not hasattr(_Event, "is_set"):     # pragma: no cover
        is_set = _Event.isSet


class Thread(_Thread):

    if not hasattr(_Thread, "is_alive"):  # pragma: no cover
        is_alive = _Thread.isAlive

    if not hasattr(_Thread, "daemon"):    # pragma: no cover
        daemon = property(_Thread.isDaemon, _Thread.setDaemon)

    if not hasattr(_Thread, "name"):      # pragma: no cover
        name = property(_Thread.getName, _Thread.setName)


class bgThread(Thread):

    def __init__(self, name=None, **kwargs):
        super(bgThread, self).__init__()
        self._is_shutdown = Event()
        self._is_stopped = Event()
        self.daemon = True
        self.name = name or self.__class__.__name__

    def body(self):
        raise NotImplementedError("subclass responsibility")

    def on_crash(self, msg, *fmt, **kwargs):
        sys.stderr.write((msg + "\n") % fmt)
        exc_info = sys.exc_info()
        try:
            traceback.print_exception(exc_info[0], exc_info[1], exc_info[2],
                                      None, sys.stderr)
        finally:
            del(exc_info)

    def run(self):
        body = self.body
        shutdown_set = self._is_shutdown.is_set
        try:
            while not shutdown_set():
                try:
                    body()
                except Exception, exc:
                    try:
                        self.on_crash("%r crashed: %r", self.name, exc)
                        self._set_stopped()
                    finally:
                        os._exit(1)  # exiting by normal means won't work
        finally:
            self._set_stopped()

    def _set_stopped(self):
        try:
            self._is_stopped.set()
        except TypeError:  # pragma: no cover
            # we lost the race at interpreter shutdown,
            # so gc collected built-in modules.
            pass

    def stop(self):
        """Graceful shutdown."""
        self._is_shutdown.set()
        self._is_stopped.wait()
        if self.is_alive():
            self.join(1e100)
