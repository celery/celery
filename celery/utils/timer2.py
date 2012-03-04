# -*- coding: utf-8 -*-
"""
    timer2
    ~~~~~~

    Scheduler for Python functions.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import atexit
import heapq
import logging
import os
import sys
import traceback
import warnings

from itertools import count
from threading import Condition, Event, Lock, Thread
from time import time, sleep, mktime

from datetime import datetime, timedelta

VERSION = (1, 0, 0)
__version__ = ".".join(map(str, VERSION))
__author__ = "Ask Solem"
__contact__ = "ask@celeryproject.org"
__homepage__ = "http://github.com/ask/timer2/"
__docformat__ = "restructuredtext"

DEFAULT_MAX_INTERVAL = 2


class TimedFunctionFailed(UserWarning):
    pass


class Entry(object):
    cancelled = False

    def __init__(self, fun, args=None, kwargs=None):
        self.fun = fun
        self.args = args or []
        self.kwargs = kwargs or {}
        self.tref = self

    def __call__(self):
        return self.fun(*self.args, **self.kwargs)

    def cancel(self):
        self.tref.cancelled = True

    def __repr__(self):
        return "<TimerEntry: %s(*%r, **%r)" % (
                self.fun.__name__, self.args, self.kwargs)

    if sys.version_info >= (3, 0):

        def __hash__(self):
            return hash("|".join(map(repr, (self.fun, self.args,
                                            self.kwargs))))

        def __lt__(self, other):
            return hash(self) < hash(other)

        def __gt__(self, other):
            return hash(self) > hash(other)

        def __eq__(self, other):
            return hash(self) == hash(other)


def to_timestamp(d):
    if isinstance(d, datetime):
        return mktime(d.timetuple())
    return d


class Schedule(object):
    """ETA scheduler."""
    on_error = None

    def __init__(self, max_interval=DEFAULT_MAX_INTERVAL, on_error=None):
        self.max_interval = float(max_interval)
        self.on_error = on_error or self.on_error
        self._queue = []

    def handle_error(self, exc_info):
        if self.on_error:
            self.on_error(exc_info)
            return True

    def enter(self, entry, eta=None, priority=0):
        """Enter function into the scheduler.

        :param entry: Item to enter.
        :keyword eta: Scheduled time as a :class:`datetime.datetime` object.
        :keyword priority: Unused.

        """
        if eta is None:  # schedule now
            eta = datetime.now()

        try:
            eta = to_timestamp(eta)
        except OverflowError:
            if not self.handle_error(sys.exc_info()):
                raise

        if eta is None:
            # schedule now.
            eta = time()

        heapq.heappush(self._queue, (eta, priority, entry))
        return entry

    def __iter__(self):
        """The iterator yields the time to sleep for between runs."""

        # localize variable access
        nowfun = time
        pop = heapq.heappop
        max_interval = self.max_interval
        queue = self._queue

        while 1:
            if queue:
                eta, priority, entry = verify = queue[0]
                now = nowfun()

                if now < eta:
                    yield min(eta - now, max_interval), None
                else:
                    event = pop(queue)

                    if event is verify:
                        if not entry.cancelled:
                            yield None, entry
                        continue
                    else:
                        heapq.heappush(queue, event)
            yield None, None

    def empty(self):
        """Is the schedule empty?"""
        return not self._queue

    def clear(self):
        self._queue[:] = []  # used because we can't replace the object
                             # and the operation is atomic.

    def info(self):
        return ({"eta": eta, "priority": priority, "item": item}
                    for eta, priority, item in self.queue)

    @property
    def queue(self):
        events = list(self._queue)
        return map(heapq.heappop, [events] * len(events))


class Timer(Thread):
    Entry = Entry
    Schedule = Schedule

    running = False
    on_tick = None
    _timer_count = count(1).next

    def __init__(self, schedule=None, on_error=None, on_tick=None, **kwargs):
        self.schedule = schedule or self.Schedule(on_error=on_error)
        self.on_tick = on_tick or self.on_tick

        Thread.__init__(self)
        self._is_shutdown = Event()
        self._is_stopped = Event()
        self.mutex = Lock()
        self.logger = logging.getLogger("timer2.Timer")
        self.not_empty = Condition(self.mutex)
        self.setDaemon(True)
        self.setName("Timer-%s" % (self._timer_count(), ))

    def apply_entry(self, entry):
        try:
            entry()
        except Exception, exc:
            exc_info = sys.exc_info()
            try:
                if not self.schedule.handle_error(exc_info):
                    warnings.warn(TimedFunctionFailed(repr(exc))),
                    sys.stderr.write("Error in timer: %r\n" % (exc, ))
                    traceback.print_exception(exc_info[0],
                                              exc_info[1],
                                              exc_info[2],
                                              None, sys.__stderr__)
            finally:
                del(exc_info)

    def _next_entry(self):
        with self.not_empty:
            delay, entry = self.scheduler.next()
            if entry is None:
                if delay is None:
                    self.not_empty.wait(1.0)
                return delay
        return self.apply_entry(entry)
    __next__ = next = _next_entry  # for 2to3

    def run(self):
        try:
            self.running = True
            self.scheduler = iter(self.schedule)

            while not self._is_shutdown.isSet():
                delay = self._next_entry()
                if delay:
                    if self.on_tick:
                        self.on_tick(delay)
                    if sleep is None:  # pragma: no cover
                        break
                    sleep(delay)
            try:
                self._is_stopped.set()
            except TypeError:  # pragma: no cover
                # we lost the race at interpreter shutdown,
                # so gc collected built-in modules.
                pass
        except Exception, exc:
            self.logger.error("Thread Timer crashed: %r", exc,
                              exc_info=True)
            os._exit(1)

    def stop(self):
        if self.running:
            self._is_shutdown.set()
            self._is_stopped.wait()
            self.join(1e10)
            self.running = False

    def ensure_started(self):
        if not self.running and not self.isAlive():
            self.start()

    def enter(self, entry, eta, priority=None):
        self.ensure_started()
        with self.mutex:
            entry = self.schedule.enter(entry, eta, priority)
            self.not_empty.notify()
            return entry

    def apply_at(self, eta, fun, args=(), kwargs={}, priority=0):
        return self.enter(self.Entry(fun, args, kwargs), eta, priority)

    def enter_after(self, msecs, entry, priority=0):
        eta = datetime.now() + timedelta(seconds=msecs / 1000.0)
        return self.enter(entry, eta, priority)

    def apply_after(self, msecs, fun, args=(), kwargs={}, priority=0):
        return self.enter_after(msecs, Entry(fun, args, kwargs), priority)

    def apply_interval(self, msecs, fun, args=(), kwargs={}, priority=0):
        tref = Entry(fun, args, kwargs)

        def _reschedules(*args, **kwargs):
            try:
                return fun(*args, **kwargs)
            finally:
                if not tref.cancelled:
                    self.enter_after(msecs, tref, priority)

        tref.fun = _reschedules
        return self.enter_after(msecs, tref, priority)

    def exit_after(self, msecs, priority=10):
        self.apply_after(msecs, sys.exit, priority)

    def cancel(self, tref):
        tref.cancel()

    def clear(self):
        self.schedule.clear()

    def empty(self):
        return self.schedule.empty()

    @property
    def queue(self):
        return self.schedule.queue

default_timer = _default_timer = Timer()
apply_after = _default_timer.apply_after
apply_at = _default_timer.apply_at
apply_interval = _default_timer.apply_interval
enter_after = _default_timer.enter_after
enter = _default_timer.enter
exit_after = _default_timer.exit_after
cancel = _default_timer.cancel
clear = _default_timer.clear

atexit.register(_default_timer.stop)
