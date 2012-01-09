from __future__ import absolute_import

import os
import sys
import threading

_Thread = threading.Thread
_Event = threading._Event


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

    def run(self):
        shutdown = self._is_shutdown
        while not shutdown.is_set():
            try:
                self.body()
            except Exception, exc:
                self.on_crash("%r crashed: %r", self.name, exc, exc_info=True)
                # exiting by normal means does not work here, so force exit.
                os._exit(1)
        try:
            self._is_stopped.set()
        except TypeError:  # pragma: no cover
            # we lost the race at interpreter shutdown,
            # so gc collected built-in modules.
            pass
        self._is_stopped.set()

    def stop(self):
        """Graceful shutdown."""
        self._is_shutdown.set()
        self._is_stopped.wait()
        if self.is_alive():
            self.join(1e100)
