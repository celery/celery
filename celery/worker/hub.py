from __future__ import absolute_import

from kombu.utils import cached_property
from kombu.utils.eventio import poll, POLL_READ, POLL_ERR

from celery.utils.timer2 import Schedule


class Hub(object):
    eventflags = POLL_READ | POLL_ERR

    def __init__(self, schedule=None):
        self.fdmap = {}
        self.poller = poll()
        self.schedule = Schedule() if schedule is None else schedule

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return self.close()

    def fire_timers(self, min_delay=10, max_delay=10):
        delay, entry = self.scheduler.next()
        if entry is not None:
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
