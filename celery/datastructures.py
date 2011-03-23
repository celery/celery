"""
celery.datastructures
=====================

Custom data structures.

:copyright: (c) 2009 - 2011 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import generators

import time
import traceback

from itertools import chain
from Queue import Empty

from celery.utils.compat import OrderedDict


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
    """A view over an applications configuration dicts.

    If the key does not exist in ``changes``, the ``defaults`` dict
    is consulted.

    :param changes:  Dict containing changes to the configuration.
    :param defaults: Dict containing the default configuration.

    """
    changes = None
    defaults = None

    def __init__(self, changes, defaults):
        self.__dict__["changes"] = changes
        self.__dict__["defaults"] = defaults
        self.__dict__["_order"] = [changes] + defaults

    def __getitem__(self, key):
        for d in self.__dict__["_order"]:
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
        for d in self.__dict__["_order"]:
            if key in d:
                return True
        return False

    def __repr__(self):
        return repr(dict(self.iteritems()))

    def __iter__(self):
        return self.iterkeys()

    def _iter(self, op):
        # defaults must be first in the stream, so values in
        # changes takes precedence.
        return chain(*[op(d) for d in reversed(self.__dict__["_order"])])

    def iterkeys(self):
        return self._iter(lambda d: d.iterkeys())

    def iteritems(self):
        return self._iter(lambda d: d.iteritems())

    def itervalues(self):
        return self._iter(lambda d: d.itervalues())

    def keys(self):
        return list(self.iterkeys())

    def items(self):
        return list(self.iteritems())

    def values(self):
        return list(self.itervalues())


class ExceptionInfo(object):
    """Exception wrapping an exception and its traceback.

    :param exc_info: The exception info tuple as returned by
        :func:`sys.exc_info`.

    """

    #: The original exception.
    exception = None

    #: A traceback form the point when :attr:`exception` was raised.
    traceback = None

    def __init__(self, exc_info):
        _, exception, _ = exc_info
        self.exception = exception
        self.traceback = ''.join(traceback.format_exception(*exc_info))

    def __str__(self):
        return self.traceback

    def __repr__(self):
        return "<ExceptionInfo: %r>" % (self.exception, )


def consume_queue(queue):
    """Iterator yielding all immediately available items in a
    :class:`Queue.Queue`.

    The iterator stops as soon as the queue raises :exc:`Queue.Empty`.

    *Examples*

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
        except Empty:
            break


class LimitedSet(object):
    """Kind-of Set with limitations.

    Good for when you need to test for membership (`a in set`),
    but the list might become to big, so you want to limit it so it doesn't
    consume too much resources.

    :keyword maxlen: Maximum number of members before we start
                     evicting expired members.
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
                    except TypeError:  # pragma: no cover
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

        This implementation may not be thread safe.

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
