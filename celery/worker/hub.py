from __future__ import absolute_import

from collections import deque

from kombu.utils import cached_property
from kombu.utils.eventio import poll, POLL_READ, POLL_ERR

from celery.utils.timer2 import Schedule


class BoundedSemaphore(object):

    def __init__(self, value=1):
        self.initial_value = self.value = value
        self._waiting = set()

    def grow(self):
        self.initial_value += 1

    def shrink(self):
        self.initial_value -= 1

    def acquire(self, callback, *partial_args, **partial_kwargs):
        if self.value <= 0:
            self._waiting.add((callback, partial_args))
            return False
        else:
            self.value = max(self.value - 1, 0)
            callback(*partial_args, **partial_kwargs)
            return True

    def release(self):
        self.value = min(self.value + 1, self.initial_value)
        if self._waiting:
            waiter, args = self._waiting.pop()
            waiter(*args)


    def clear(self):
        pass


class Hub(object):
    eventflags = POLL_READ | POLL_ERR

    def __init__(self, schedule=None):
        self.fdmap = {}
        self.poller = poll()
        self.schedule = Schedule() if schedule is None else schedule
        self._on_event = set()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return self.close()

    def fire_timers(self, min_delay=10, max_delay=10):
        while 1:
            delay, entry = self.scheduler.next()
            if entry is None:
                break
            self.schedule.apply_entry(entry)
        return min(max(delay, min_delay), max_delay)

    def add(self, fd, callback, flags=None):
        flags = self.eventflags if flags is None else flags
        self.poller.register(fd, flags)
        try:
            fileno = fd.fileno()
        except AttributeError:
            fileno = fd
        self.fdmap[fileno] = callback

    def update(self, *maps):
        [self.add(*x) for row in maps for x in row.iteritems()]

    def remove(self, fd):
        try:
            self.poller.unregister(fd)
        except (KeyError, OSError):
            pass

    def close(self):
        [self.remove(fd) for fd in self.fdmap.keys()]

    @cached_property
    def scheduler(self):
        return iter(self.schedule)
