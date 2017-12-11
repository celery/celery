# -*- coding: utf-8 -*-
"""Scheduler for Python functions.

.. note::
    This is used for the thread-based worker only,
    not for amqp/redis/sqs/qpid where :mod:`kombu.async.timer` is used.
"""
import os
import sys
import threading
from itertools import count
from numbers import Number
from time import sleep
<<<<<<< HEAD

from kombu.async.timer import Entry
from kombu.async.timer import Timer as Schedule
from kombu.async.timer import logger, to_timestamp
=======
from typing import Callable, MutableSequence, Optional, Union
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726

from celery.five import THREAD_TIMEOUT_MAX

TIMER_DEBUG = os.environ.get('TIMER_DEBUG')

__all__ = ('Entry', 'Schedule', 'Timer', 'to_timestamp')


class Timer(threading.Thread):
    """Timer thread.

    Note:
        This is only used for transports not supporting AsyncIO.
    """

    Entry = Entry
    Schedule = Schedule

    running = False
    on_tick = None

    _timer_count = count(1)

    if TIMER_DEBUG:  # pragma: no cover
        def start(self, *args, **kwargs) -> None:
            import traceback
            print('- Timer starting')
            traceback.print_stack()
            super().start(*args, **kwargs)

    def __init__(self, schedule: Schedule = None,
                 on_error: Callable = None,
                 on_tick: Callable = None,
                 on_start: Callable = None,
                 max_interval: Number = None, **kwargs) -> None:
        self.schedule = schedule or self.Schedule(on_error=on_error,
                                                  max_interval=max_interval)
        self.on_start = on_start
        self.on_tick = on_tick or self.on_tick
        threading.Thread.__init__(self)
        self._is_shutdown = threading.Event()
        self._is_stopped = threading.Event()
        self.mutex = threading.Lock()
        self.not_empty = threading.Condition(self.mutex)
        self.daemon = True
        self.name = 'Timer-{0}'.format(next(self._timer_count))

    def _next_entry(self) -> Optional[Number]:
        with self.not_empty:
            delay, entry = next(self.scheduler)
            if entry is None:
                if delay is None:
                    self.not_empty.wait(1.0)
                return delay
        return self.schedule.apply_entry(entry)
    __next__ = next = _next_entry  # for 2to3

    def run(self) -> None:
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
        except Exception as exc:
            logger.error('Thread Timer crashed: %r', exc, exc_info=True)
            os._exit(1)

    def stop(self) -> None:
        self._is_shutdown.set()
        if self.running:
            self._is_stopped.wait()
            self.join(threading.TIMEOUT_MAX)
            self.running = False

    def ensure_started(self) -> None:
        if not self.running and not self.isAlive():
            if self.on_start:
                self.on_start(self)
            self.start()

    def _do_enter(self, meth: str, *args, **kwargs) -> Entry:
        self.ensure_started()
        with self.mutex:
            entry = getattr(self.schedule, meth)(*args, **kwargs)
            self.not_empty.notify()
            return entry

    def enter(self, entry: Entry, eta: float,
              priority: int = None) -> Entry:
        return self._do_enter('enter_at', entry, eta, priority=priority)

    def call_at(self, *args, **kwargs) -> Entry:
        return self._do_enter('call_at', *args, **kwargs)

    def enter_after(self, *args, **kwargs) -> Entry:
        return self._do_enter('enter_after', *args, **kwargs)

    def call_after(self, *args, **kwargs) -> Entry:
        return self._do_enter('call_after', *args, **kwargs)

    def call_repeatedly(self, *args, **kwargs) -> Entry:
        return self._do_enter('call_repeatedly', *args, **kwargs)

    def exit_after(self, secs: Union[int, float],
                   priority: int = 10) -> None:
        self.call_after(secs, sys.exit, priority)

    def cancel(self, tref: Entry) -> None:
        tref.cancel()

    def clear(self) -> None:
        self.schedule.clear()

    def empty(self) -> bool:
        return not len(self)

    def __len__(self) -> int:
        return len(self.schedule)

    def __bool__(self) -> bool:
        return True

    @property
    def queue(self) -> MutableSequence:
        return self.schedule.queue
