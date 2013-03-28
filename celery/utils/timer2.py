# -*- coding: utf-8 -*-
"""
    timer2
    ~~~~~~

    Scheduler for Python functions.

"""
from __future__ import absolute_import
from __future__ import with_statement

import atexit
import heapq
import os
import sys
import threading

from datetime import datetime
from functools import wraps
from itertools import count
from time import time, sleep
from weakref import proxy as weakrefproxy

from celery.utils.compat import THREAD_TIMEOUT_MAX
from celery.utils.timeutils import timedelta_seconds, timezone
from kombu.log import get_logger

VERSION = (1, 0, 0)
__version__ = '.'.join(str(p) for p in VERSION)
__author__ = 'Ask Solem'
__contact__ = 'ask@celeryproject.org'
__homepage__ = 'http://github.com/ask/timer2/'
__docformat__ = 'restructuredtext'

DEFAULT_MAX_INTERVAL = 2
TIMER_DEBUG = os.environ.get('TIMER_DEBUG')
EPOCH = datetime.utcfromtimestamp(0).replace(tzinfo=timezone.utc)
IS_PYPY = hasattr(sys, 'pypy_version_info')

logger = get_logger('timer2')


class Entry(object):
    if not IS_PYPY:
        __slots__ = (
            'fun', 'args', 'kwargs', 'tref', 'cancelled',
            '_last_run', '__weakref__',
        )

    def __init__(self, fun, args=None, kwargs=None):
        self.fun = fun
        self.args = args or []
        self.kwargs = kwargs or {}
        self.tref = weakrefproxy(self)
        self._last_run = None
        self.cancelled = False

    def __call__(self):
        return self.fun(*self.args, **self.kwargs)

    def cancel(self):
        try:
            self.tref.cancelled = True
        except ReferenceError:
            pass

    def __repr__(self):
        return '<TimerEntry: %s(*%r, **%r)' % (
            self.fun.__name__, self.args, self.kwargs)

    if sys.version_info[0] == 3:  # pragma: no cover

        def __hash__(self):
            return hash('|'.join(
                repr(v) for v in (self.fun, self.args, self.kwargs)
            ))

        def __lt__(self, other):
            return hash(self) < hash(other)

        def __gt__(self, other):
            return hash(self) > hash(other)

        def __eq__(self, other):
            return hash(self) == hash(other)


def to_timestamp(d, default_timezone=timezone.utc):
    if isinstance(d, datetime):
        if d.tzinfo is None:
            d = d.replace(tzinfo=default_timezone)
        return timedelta_seconds(d - EPOCH)
    return d


class Schedule(object):
    """ETA scheduler."""
    Entry = Entry

    on_error = None

    def __init__(self, max_interval=None, on_error=None, **kwargs):
        self.max_interval = float(max_interval or DEFAULT_MAX_INTERVAL)
        self.on_error = on_error or self.on_error
        self._queue = []

    def apply_entry(self, entry):
        try:
            entry()
        except Exception, exc:
            if not self.handle_error(exc):
                logger.error('Error in timer: %r', exc, exc_info=True)

    def handle_error(self, exc_info):
        if self.on_error:
            self.on_error(exc_info)
            return True

    def stop(self):
        pass

    def enter(self, entry, eta=None, priority=0):
        """Enter function into the scheduler.

        :param entry: Item to enter.
        :keyword eta: Scheduled time as a :class:`datetime.datetime` object.
        :keyword priority: Unused.

        """
        if eta is None:
            eta = time()
        if isinstance(eta, datetime):
            try:
                eta = to_timestamp(eta)
            except Exception, exc:
                if not self.handle_error(exc):
                    raise
                return
        return self._enter(eta, priority, entry)

    def _enter(self, eta, priority, entry):
        heapq.heappush(self._queue, (eta, priority, entry))
        return entry

    def apply_at(self, eta, fun, args=(), kwargs={}, priority=0):
        return self.enter(self.Entry(fun, args, kwargs), eta, priority)

    def enter_after(self, msecs, entry, priority=0, time=time):
        return self.enter(entry, time() + (msecs / 1000.0), priority)

    def apply_after(self, msecs, fun, args=(), kwargs={}, priority=0):
        return self.enter_after(msecs, self.Entry(fun, args, kwargs), priority)

    def apply_interval(self, msecs, fun, args=(), kwargs={}, priority=0):
        tref = self.Entry(fun, args, kwargs)
        secs = msecs * 1000.0

        @wraps(fun)
        def _reschedules(*args, **kwargs):
            last, now = tref._last_run, time()
            lsince = (now - tref._last_run) * 1000.0 if last else msecs
            try:
                if lsince and lsince >= msecs:
                    tref._last_run = now
                    return fun(*args, **kwargs)
            finally:
                if not tref.cancelled:
                    last = tref._last_run
                    next = secs - (now - last) if last else secs
                    self.enter_after(next / 1000.0, tref, priority)

        tref.fun = _reschedules
        tref._last_run = None
        return self.enter_after(msecs, tref, priority)

    @property
    def schedule(self):
        return self

    def __iter__(self, min=min, nowfun=time, pop=heapq.heappop,
                 push=heapq.heappush):
        """The iterator yields the time to sleep for between runs."""
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
                        push(queue, event)
            else:
                yield None, None

    def empty(self):
        """Is the schedule empty?"""
        return not self._queue

    def clear(self):
        self._queue[:] = []  # used because we can't replace the object
                             # and the operation is atomic.

    def info(self):
        return ({'eta': eta, 'priority': priority, 'item': item}
                for eta, priority, item in self.queue)

    def cancel(self, tref):
        tref.cancel()

    @property
    def queue(self, _pop=heapq.heappop):
        """Snapshot of underlying datastructure."""
        events = list(self._queue)
        return [_pop(i) for i in [events] * len(events)]


class Timer(threading.Thread):
    Entry = Entry
    Schedule = Schedule

    running = False
    on_tick = None
    _timer_count = count(1).next

    if TIMER_DEBUG:  # pragma: no cover
        def start(self, *args, **kwargs):
            import traceback
            print('- Timer starting')
            traceback.print_stack()
            super(Timer, self).start(*args, **kwargs)

    def __init__(self, schedule=None, on_error=None, on_tick=None,
                 max_interval=None, **kwargs):
        self.schedule = schedule or self.Schedule(on_error=on_error,
                                                  max_interval=max_interval)
        self.on_tick = on_tick or self.on_tick

        threading.Thread.__init__(self)
        self._is_shutdown = threading.Event()
        self._is_stopped = threading.Event()
        self.mutex = threading.Lock()
        self.not_empty = threading.Condition(self.mutex)
        self.setDaemon(True)
        self.setName('Timer-%s' % (self._timer_count(), ))

    def _next_entry(self):
        with self.not_empty:
            delay, entry = self.scheduler.next()
            if entry is None:
                if delay is None:
                    self.not_empty.wait(1.0)
                return delay
        return self.schedule.apply_entry(entry)
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
            logger.error('Thread Timer crashed: %r', exc, exc_info=True)
            os._exit(1)

    def stop(self):
        if self.running:
            self._is_shutdown.set()
            self._is_stopped.wait()
            self.join(THREAD_TIMEOUT_MAX)
            self.running = False

    def ensure_started(self):
        if not self.running and not self.isAlive():
            self.start()

    def _do_enter(self, meth, *args, **kwargs):
        self.ensure_started()
        with self.mutex:
            entry = getattr(self.schedule, meth)(*args, **kwargs)
            self.not_empty.notify()
            return entry

    def enter(self, entry, eta, priority=None):
        return self._do_enter('enter', entry, eta, priority=priority)

    def apply_at(self, *args, **kwargs):
        return self._do_enter('apply_at', *args, **kwargs)

    def enter_after(self, *args, **kwargs):
        return self._do_enter('enter_after', *args, **kwargs)

    def apply_after(self, *args, **kwargs):
        return self._do_enter('apply_after', *args, **kwargs)

    def apply_interval(self, *args, **kwargs):
        return self._do_enter('apply_interval', *args, **kwargs)

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
