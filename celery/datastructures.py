"""
celery.datastructures
=====================

Custom data structures.

:copyright: (c) 2009 - 2010 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import generators

import time
import traceback

from itertools import chain
from Queue import Queue, Empty as QueueEmpty

from celery.utils.compat import OrderedDict, UserList


class AttributeDictMixin(object):
    """Adds attribute access to mappings.

    `d.key -> d[key]`

    """

    def __getattr__(self, key):
        """`d.key -> d[key]`"""
        try:
            return self[key]
        except KeyError:
            raise AttributeError("'%s' object has no attribute '%s'" % (
                    self.__class__.__name__, key))

    def __setattr__(self, key, value):
        """`d[key] = value -> d.key = value`"""
        self[key] = value


class AttributeDict(dict, AttributeDictMixin):
    """Dict subclass with attribute access."""
    pass


class DictAttribute(object):
    """Dict interface to attributes.

    `obj[k] -> obj.k`

    """

    def __init__(self, obj):
        self.obj = obj

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def setdefault(self, key, default):
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    def __getitem__(self, key):
        try:
            return getattr(self.obj, key)
        except AttributeError:
            raise KeyError(key)

    def __setitem__(self, key, value):
        setattr(self.obj, key, value)

    def __contains__(self, key):
        return hasattr(self.obj, key)

    def iteritems(self):
        return vars(self.obj).iteritems()


class ConfigurationView(AttributeDictMixin):
    changes = None
    defaults = None

    def __init__(self, changes, defaults):
        self.__dict__["changes"] = changes
        self.__dict__["defaults"] = defaults

    def __getitem__(self, key):
        for d in self.__dict__["changes"], self.__dict__["defaults"]:
            try:
                return d[key]
            except KeyError:
                pass
        raise KeyError(key)

    def __setitem__(self, key, value):
        self.__dict__["changes"][key] = value

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def setdefault(self, key, default):
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    def update(self, *args, **kwargs):
        return self.__dict__["changes"].update(*args, **kwargs)

    def __contains__(self, key):
        for d in self.__dict__["changes"], self.__dict__["defaults"]:
            if key in d:
                return True
        return False

    def __repr__(self):
        return repr(dict(iter(self)))

    def __iter__(self):
        # defaults must be first in the stream, so values in
        # in changes takes precedence.
        return chain(*[d.iteritems() for d in (self.__dict__["defaults"],
                                               self.__dict__["changes"])])

    def iteritems(self):
        return iter(self)

    def items(self):
        return tuple(self.iteritems())


class ExceptionInfo(object):
    """Exception wrapping an exception and its traceback.

    :param exc_info: The exception tuple info as returned by
        :func:`traceback.format_exception`.

    """

    #: The original exception.
    exception = None

    #: A traceback form the point when :attr:`exception` was raised.
    traceback = None

    def __init__(self, exc_info):
        type_, exception, tb = exc_info
        self.exception = exception
        self.traceback = ''.join(traceback.format_exception(*exc_info))

    def __str__(self):
        return self.traceback

    def __repr__(self):
        return "<%s.%s: %s>" % (
                self.__class__.__module__,
                self.__class__.__name__,
                str(self.exception))


def consume_queue(queue):
    """Iterator yielding all immediately available items in a
    :class:`Queue.Queue`.

    The iterator stops as soon as the queue raises :exc:`Queue.Empty`.

    Example

        >>> q = Queue()
        >>> map(q.put, range(4))
        >>> list(consume_queue(q))
        [0, 1, 2, 3]
        >>> list(consume_queue(q))
        []

    """
    while 1:
        try:
            yield queue.get_nowait()
        except QueueEmpty:
            break


class SharedCounter(object):
    """Thread-safe counter.

    Please note that the final value is not synchronized, this means
    that you should not update the value by using a previous value, the only
    reliable operations are increment and decrement.

    Example::

        >>> max_clients = SharedCounter(initial_value=10)

        # Thread one
        >>> max_clients += 1 # OK (safe)

        # Thread two
        >>> max_clients -= 3 # OK (safe)

        # Main thread
        >>> if client >= int(max_clients): # Max clients now at 8
        ...    wait()

        >>> max_client = max_clients + 10 # NOT OK (unsafe)

    """

    def __init__(self, initial_value):
        self._value = initial_value
        self._modify_queue = Queue()

    def increment(self, n=1):
        """Increment value."""
        self += n
        return int(self)

    def decrement(self, n=1):
        """Decrement value."""
        self -= n
        return int(self)

    def _update_value(self):
        self._value += sum(consume_queue(self._modify_queue))
        return self._value

    def __iadd__(self, y):
        """`self += y`"""
        self._modify_queue.put(y * +1)
        return self

    def __isub__(self, y):
        """`self -= y`"""
        self._modify_queue.put(y * -1)
        return self

    def __int__(self):
        """`int(self) -> int`"""
        return self._update_value()

    def __repr__(self):
        return "<SharedCounter: int(%s)>" % str(int(self))


class LimitedSet(object):
    """Kind-of Set with limitations.

    Good for when you need to test for membership (`a in set`),
    but the list might become to big, so you want to limit it so it doesn't
    consume too much resources.

    :keyword maxlen: Maximum number of members before we start
                     deleting expired members.
    :keyword expires: Time in seconds, before a membership expires.

    """

    def __init__(self, maxlen=None, expires=None):
        self.maxlen = maxlen
        self.expires = expires
        self._data = {}

    def add(self, value):
        """Add a new member."""
        self._expire_item()
        self._data[value] = time.time()

    def clear(self):
        """Remove all members"""
        self._data.clear()

    def pop_value(self, value):
        """Remove membership by finding value."""
        self._data.pop(value, None)

    def _expire_item(self):
        """Hunt down and remove an expired item."""
        while 1:
            if self.maxlen and len(self) >= self.maxlen:
                value, when = self.first
                if not self.expires or time.time() > when + self.expires:
                    try:
                        self.pop_value(value)
                    except TypeError:                   # pragma: no cover
                        continue
            break

    def __contains__(self, value):
        return value in self._data

    def update(self, other):
        if isinstance(other, self.__class__):
            self._data.update(other._data)
        else:
            self._data.update(other)

    def as_dict(self):
        return self._data

    def __iter__(self):
        return iter(self._data.keys())

    def __len__(self):
        return len(self._data.keys())

    def __repr__(self):
        return "LimitedSet([%s])" % (repr(self._data.keys()))

    @property
    def chronologically(self):
        return sorted(self._data.items(), key=lambda (value, when): when)

    @property
    def first(self):
        """Get the oldest member."""
        return self.chronologically[0]


class LocalCache(OrderedDict):
    """Dictionary with a finite number of keys.

    Older items expires first.

    """

    def __init__(self, limit=None):
        super(LocalCache, self).__init__()
        self.limit = limit

    def __setitem__(self, key, value):
        while len(self) >= self.limit:
            self.popitem(last=False)
        super(LocalCache, self).__setitem__(key, value)


class TokenBucket(object):
    """Token Bucket Algorithm.

    See http://en.wikipedia.org/wiki/Token_Bucket
    Most of this code was stolen from an entry in the ASPN Python Cookbook:
    http://code.activestate.com/recipes/511490/

    .. admonition:: Thread safety

        This implementation is not thread safe.

    :param fill_rate: Refill rate in tokens/second.
    :keyword capacity: Max number of tokens.  Default is 1.

    """

    #: The rate in tokens/second that the bucket will be refilled
    fill_rate = None

    #: Maximum number of tokensin the bucket.
    capacity = 1

    #: Timestamp of the last time a token was taken out of the bucket.
    timestamp = None

    def __init__(self, fill_rate, capacity=1):
        self.capacity = float(capacity)
        self._tokens = capacity
        self.fill_rate = float(fill_rate)
        self.timestamp = time.time()

    def can_consume(self, tokens=1):
        """Returns :const:`True` if `tokens` number of tokens can be consumed
        from the bucket."""
        if tokens <= self._get_tokens():
            self._tokens -= tokens
            return True
        return False

    def expected_time(self, tokens=1):
        """Returns the expected time in seconds when a new token should be
        available.

        .. admonition:: Warning

            This consumes a token from the bucket.

        """
        _tokens = self._get_tokens()
        tokens = max(tokens, _tokens)
        return (tokens - _tokens) / self.fill_rate

    def _get_tokens(self):
        if self._tokens < self.capacity:
            now = time.time()
            delta = self.fill_rate * (now - self.timestamp)
            self._tokens = min(self.capacity, self._tokens + delta)
            self.timestamp = now
        return self._tokens
