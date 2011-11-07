# -*- coding: utf-8 -*-
"""
    celery.datastructures
    ~~~~~~~~~~~~~~~~~~~~~

    Custom types and data structures.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import sys
import time
import traceback

from itertools import chain
from threading import RLock

from .utils.compat import UserDict, OrderedDict


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

    def _iterate_items(self):
        return vars(self.obj).iteritems()
    iteritems = _iterate_items

    if sys.version_info >= (3, 0):
        items = _iterate_items
    else:

        def items(self):
            return list(self._iterate_items())


class ConfigurationView(AttributeDictMixin):
    """A view over an applications configuration dicts.

    If the key does not exist in ``changes``, the ``defaults`` dict
    is consulted.

    :param changes:  Dict containing changes to the configuration.
    :param defaults: Dict containing the default configuration.

    """
    changes = None
    defaults = None
    _order = None

    def __init__(self, changes, defaults):
        self.__dict__.update(changes=changes, defaults=defaults,
                             _order=[changes] + defaults)

    def __getitem__(self, key):
        for d in self._order:
            try:
                return d[key]
            except KeyError:
                pass
        raise KeyError(key)

    def __setitem__(self, key, value):
        self.changes[key] = value

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
        return self.changes.update(*args, **kwargs)

    def __contains__(self, key):
        for d in self._order:
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
        return chain(*[op(d) for d in reversed(self._order)])

    def _iterate_keys(self):
        return self._iter(lambda d: d.iterkeys())
    iterkeys = _iterate_keys

    def _iterate_items(self):
        return self._iter(lambda d: d.iteritems())
    iteritems = _iterate_items

    def _iterate_values(self):
        return self._iter(lambda d: d.itervalues())
    itervalues = _iterate_values

    def keys(self):
        return list(self._iterate_keys())

    def items(self):
        return list(self._iterate_items())

    def values(self):
        return list(self._iterate_values())


class _Code(object):

    def __init__(self, code):
        self.co_filename = code.co_filename
        self.co_name = code.co_name


class _Frame(object):
    Code = _Code

    def __init__(self, frame):
        self.f_globals = {
            "__file__": frame.f_globals.get("__file__", "__main__"),
        }
        self.f_code = self.Code(frame.f_code)


class Traceback(object):
    Frame = _Frame

    def __init__(self, tb):
        self.tb_frame = self.Frame(tb.tb_frame)
        self.tb_lineno = tb.tb_lineno
        if tb.tb_next is None:
            self.tb_next = None
        else:
            self.tb_next = Traceback(tb.tb_next)


class ExceptionInfo(object):
    """Exception wrapping an exception and its traceback.

    :param exc_info: The exception info tuple as returned by
        :func:`sys.exc_info`.

    """

    #: Exception type.
    type = None

    #: Exception instance.
    exception = None

    #: Pickleable traceback instance for use with :mod:`traceback`
    tb = None

    #: String representation of the traceback.
    traceback = None

    def __init__(self, exc_info):
        self.type, self.exception, tb = exc_info
        self.tb = Traceback(tb)
        self.traceback = ''.join(traceback.format_exception(*exc_info))

    def __str__(self):
        return self.traceback

    def __repr__(self):
        return "<ExceptionInfo: %r>" % (self.exception, )

    @property
    def exc_info(self):
        return self.type, self.exception, self.tb


class LimitedSet(object):
    """Kind-of Set with limitations.

    Good for when you need to test for membership (`a in set`),
    but the list might become to big, so you want to limit it so it doesn't
    consume too much resources.

    :keyword maxlen: Maximum number of members before we start
                     evicting expired members.
    :keyword expires: Time in seconds, before a membership expires.

    """
    __slots__ = ("maxlen", "expires", "_data")

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


class LRUCache(UserDict):
    """LRU Cache implementation using a doubly linked list to track access.

    :keyword limit: The maximum number of keys to keep in the cache.
        When a new key is inserted and the limit has been exceeded,
        the *Least Recently Used* key will be discarded from the
        cache.

    """

    def __init__(self, limit=None):
        self.limit = limit
        self.mutex = RLock()
        self.data = OrderedDict()

    def __getitem__(self, key):
        with self.mutex:
            value = self[key] = self.data.pop(key)
            return value

    def keys(self):
        # userdict.keys in py3k calls __getitem__
        return self.data.keys()

    def values(self):
        return list(self._iterate_values())

    def items(self):
        return list(self._iterate_items())

    def __setitem__(self, key, value):
        # remove least recently used key.
        with self.mutex:
            if self.limit and len(self.data) >= self.limit:
                self.data.pop(iter(self.data).next())
            self.data[key] = value

    def __iter__(self):
        return self.data.iterkeys()

    def _iterate_items(self):
        for k in self.data:
            try:
                yield (k, self.data[k])
            except KeyError:
                pass
    iteritems = _iterate_items

    def _iterate_values(self):
        for k in self.data:
            try:
                yield self.data[k]
            except KeyError:
                pass
    itervalues = _iterate_values


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
