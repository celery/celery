# -*- coding: utf-8 -*-
"""
    celery.worker.hub
    ~~~~~~~~~~~~~~~~~

    Event-loop implementation.

"""
from __future__ import absolute_import

from kombu.utils import cached_property
from kombu.utils import eventio
from kombu.utils.eventio import READ, WRITE, ERR

from celery.five import items, range
from celery.platforms import fileno
from celery.utils.functional import maybe_list
from celery.utils.log import get_logger
from celery.utils.timer2 import Schedule

logger = get_logger(__name__)


def repr_flag(flag):
    return '{0}{1}{2}'.format('R' if flag & READ else '',
                              'W' if flag & WRITE else '',
                              '!' if flag & ERR else '')


def _rcb(obj):
    if obj is None:
        return '<missing>'
    if isinstance(obj, str):
        return obj
    return obj.__name__


class BoundedSemaphore(object):
    """Asynchronous Bounded Semaphore.

    Bounded means that the value will stay within the specified
    range even if it is released more times than it was acquired.

    This type is *not thread safe*.

    Example:

        >>> x = BoundedSemaphore(2)

        >>> def callback(i):
        ...     say('HELLO {0!r}'.format(i))

        >>> x.acquire(callback, 1)
        HELLO 1

        >>> x.acquire(callback, 2)
        HELLO 2

        >>> x.acquire(callback, 3)
        >>> x._waiters   # private, do not access directly
        [(callback, 3)]

        >>> x.release()
        HELLO 3

    """

    def __init__(self, value):
        self.initial_value = self.value = value
        self._waiting = []

    def acquire(self, callback, *partial_args):
        """Acquire semaphore, applying ``callback`` when
        the semaphore is ready.

        :param callback: The callback to apply.
        :param \*partial_args: partial arguments to callback.

        """
        if self.value <= 0:
            self._waiting.append((callback, partial_args))
            return False
        else:
            self.value = max(self.value - 1, 0)
            callback(*partial_args)
            return True

    def release(self):
        """Release semaphore.

        This will apply any waiting callbacks from previous
        calls to :meth:`acquire` done when the semaphore was busy.

        """
        self.value = min(self.value + 1, self.initial_value)
        if self._waiting:
            waiter, args = self._waiting.pop()
            waiter(*args)

    def grow(self, n=1):
        """Change the size of the semaphore to hold more values."""
        self.initial_value += n
        self.value += n
        [self.release() for _ in range(n)]

    def shrink(self, n=1):
        """Change the size of the semaphore to hold less values."""
        self.initial_value = max(self.initial_value - n, 0)
        self.value = max(self.value - n, 0)

    def clear(self):
        """Reset the sempahore, including wiping out any waiting callbacks."""
        self._waiting[:] = []
        self.value = self.initial_value


class Hub(object):
    """Event loop object.

    :keyword timer: Specify custom :class:`~celery.utils.timer2.Schedule`.

    """
    #: Flag set if reading from an fd will not block.
    READ = READ

    #: Flag set if writing to an fd will not block.
    WRITE = WRITE

    #: Flag set on error, and the fd should be read from asap.
    ERR = ERR

    #: List of callbacks to be called when the loop is initialized,
    #: applied with the hub instance as sole argument.
    on_init = None

    #: List of callbacks to be called when the loop is exiting,
    #: applied with the hub instance as sole argument.
    on_close = None

    #: List of callbacks to be called when a task is received.
    #: Takes no arguments.
    on_task = None

    def __init__(self, timer=None):
        self.timer = Schedule() if timer is None else timer

        self.readers = {}
        self.writers = {}
        self.on_init = []
        self.on_close = []
        self.on_task = []

    def start(self):
        """Called by Hub bootstep at worker startup."""
        self.poller = eventio.poll()

    def stop(self):
        """Called by Hub bootstep at worker shutdown."""
        self.poller.close()

    def init(self):
        for callback in self.on_init:
            callback(self)

    def fire_timers(self, min_delay=1, max_delay=10, max_timers=10,
                    propagate=()):
        delay = None
        if self.timer._queue:
            for i in range(max_timers):
                delay, entry = next(self.scheduler)
                if entry is None:
                    break
                try:
                    entry()
                except propagate:
                    raise
                except Exception as exc:
                    logger.error('Error in timer: %r', exc, exc_info=1)
        return min(max(delay or 0, min_delay), max_delay)

    def _add(self, fd, cb, flags):
        #if flags & WRITE:
        #    ex = self.writers.get(fd)
        #    if ex and ex.__name__ == '_write_job':
        #        assert not ex.gi_frame or ex.gi_frame == -1
        self.poller.register(fd, flags)
        (self.readers if flags & READ else self.writers)[fileno(fd)] = cb

    def add(self, fds, callback, flags):
        for fd in maybe_list(fds, None):
            try:
                self._add(fd, callback, flags)
            except ValueError:
                self._discard(fd)

    def remove(self, fd):
        fd = fileno(fd)
        self._unregister(fd)
        self._discard(fd)

    def add_reader(self, fds, callback):
        return self.add(fds, callback, READ | ERR)

    def add_writer(self, fds, callback):
        return self.add(fds, callback, WRITE)

    def update_readers(self, readers):
        [self.add_reader(*x) for x in items(readers)]

    def update_writers(self, writers):
        [self.add_writer(*x) for x in items(writers)]

    def _unregister(self, fd):
        try:
            self.poller.unregister(fd)
        except (KeyError, OSError):
            pass

    def _discard(self, fd):
        fd = fileno(fd)
        self.readers.pop(fd, None)
        self.writers.pop(fd, None)

    def close(self, *args):
        [self._unregister(fd) for fd in self.readers]
        self.readers.clear()
        [self._unregister(fd) for fd in self.writers]
        self.writers.clear()
        for callback in self.on_close:
            callback(self)

    def _repr_readers(self):
        return ['({0}){1}->{2}'.format(fd, _rcb(cb), repr_flag(READ | ERR))
                for fd, cb in items(self.readers)]

    def _repr_writers(self):
        return ['({0}){1}->{2}'.format(fd, _rcb(cb), repr_flag(WRITE))
                for fd, cb in items(self.writers)]

    def repr_active(self):
        return ', '.join(self._repr_readers() + self._repr_writers())

    def repr_events(self, events):
        return ', '.join(
            '{0}->{1}'.format(
                _rcb(self._callback_for(fd, fl, '{0!r}(GONE)'.format(fd))),
                repr_flag(fl),
            )
            for fd, fl in events
        )

    def _callback_for(self, fd, flag, *default):
        try:
            if flag & READ:
                return self.readers[fileno(fd)]
            if flag & WRITE:
                return self.writers[fileno(fd)]
        except KeyError:
            if default:
                return default[0]
            raise

    @cached_property
    def scheduler(self):
        return iter(self.timer)


class DummyLock(object):
    """Pretending to be a lock."""

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        pass
