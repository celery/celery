# -*- coding: utf-8 -*-
"""
    celery.utils.functional
    ~~~~~~~~~~~~~~~~~~~~~~~

    Utilities for functions.

"""
from __future__ import absolute_import

import operator
import threading

from functools import partial, wraps
from itertools import islice

from kombu.utils import cached_property
from kombu.utils.functional import promise, maybe_promise
from kombu.utils.compat import OrderedDict

from celery.five import UserDict, UserList, items, keys, string_t

KEYWORD_MARK = object()
is_not_None = partial(operator.is_not, None)


class LRUCache(UserDict):
    """LRU Cache implementation using a doubly linked list to track access.

    :keyword limit: The maximum number of keys to keep in the cache.
        When a new key is inserted and the limit has been exceeded,
        the *Least Recently Used* key will be discarded from the
        cache.

    """

    def __init__(self, limit=None):
        self.limit = limit
        self.mutex = threading.RLock()
        self.data = OrderedDict()

    def __getitem__(self, key):
        with self.mutex:
            value = self[key] = self.data.pop(key)
        return value

    def keys(self):
        # userdict.keys in py3k calls __getitem__
        return keys(self.data)

    def values(self):
        return list(self._iterate_values())

    def items(self):
        return list(self._iterate_items())

    def __setitem__(self, key, value):
        # remove least recently used key.
        with self.mutex:
            if self.limit and len(self.data) >= self.limit:
                self.data.pop(next(iter(self.data)))
            self.data[key] = value

    def __iter__(self):
        return iter(self.data)

    def _iterate_items(self):
        for k in self:
            try:
                yield (k, self.data[k])
            except KeyError:  # pragma: no cover
                pass
    iteritems = _iterate_items

    def _iterate_values(self):
        for k in self:
            try:
                yield self.data[k]
            except KeyError:  # pragma: no cover
                pass
    itervalues = _iterate_values

    def incr(self, key, delta=1):
        with self.mutex:
            # this acts as memcached does- store as a string, but return a
            # integer as long as it exists and we can cast it
            newval = int(self.data.pop(key)) + delta
            self[key] = str(newval)
        return newval

    def __getstate__(self):
        d = dict(vars(self))
        d.pop('mutex')
        return d

    def __setstate__(self, state):
        self.__dict__ = state
        self.mutex = threading.RLock()


def is_list(l):
    """Returns true if object is list-like, but not a dict or string."""
    return hasattr(l, '__iter__') and not isinstance(l, (dict, string_t))


def maybe_list(l):
    """Returns list of one element if ``l`` is a scalar."""
    return l if l is None or is_list(l) else [l]


def memoize(maxsize=None, Cache=LRUCache):

    def _memoize(fun):
        mutex = threading.Lock()
        cache = Cache(limit=maxsize)

        @wraps(fun)
        def _M(*args, **kwargs):
            key = args + (KEYWORD_MARK, ) + tuple(sorted(kwargs.items()))
            try:
                with mutex:
                    value = cache[key]
            except KeyError:
                value = fun(*args, **kwargs)
                _M.misses += 1
                with mutex:
                    cache[key] = value
            else:
                _M.hits += 1
            return value

        def clear():
            """Clear the cache and reset cache statistics."""
            cache.clear()
            _M.hits = _M.misses = 0

        _M.hits = _M.misses = 0
        _M.clear = clear
        _M.original_func = fun
        return _M

    return _memoize


class mpromise(promise):
    """Memoized promise.

    The function is only evaluated once, every subsequent access
    will return the same value.

    .. attribute:: evaluated

        Set to to :const:`True` after the promise has been evaluated.

    """
    evaluated = False
    _value = None

    def evaluate(self):
        if not self.evaluated:
            self._value = super(mpromise, self).evaluate()
            self.evaluated = True
        return self._value


def noop(*args, **kwargs):
    """No operation.

    Takes any arguments/keyword arguments and does nothing.

    """
    pass


def first(predicate, iterable):
    """Returns the first element in `iterable` that `predicate` returns a
    :const:`True` value for."""
    predicate = predicate or is_not_None
    for item in iterable:
        if predicate(item):
            return item


def firstmethod(method):
    """Returns a function that with a list of instances,
    finds the first instance that returns a value for the given method.

    The list can also contain promises (:class:`promise`.)

    """

    def _matcher(it, *args, **kwargs):
        for obj in it:
            try:
                answer = getattr(maybe_promise(obj), method)(*args, **kwargs)
            except AttributeError:
                pass
            else:
                if answer is not None:
                    return answer

    return _matcher


def chunks(it, n):
    """Split an iterator into chunks with `n` elements each.

    Examples

        # n == 2
        >>> x = chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), 2)
        >>> list(x)
        [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9], [10]]

        # n == 3
        >>> x = chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), 3)
        >>> list(x)
        [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]]

    """
    # XXX This function is not used anymore, at least not by Celery itself.
    for first in it:
        yield [first] + list(islice(it, n - 1))


def padlist(container, size, default=None):
    """Pad list with default elements.

    Examples:

        >>> first, last, city = padlist(['George', 'Costanza', 'NYC'], 3)
        ('George', 'Costanza', 'NYC')
        >>> first, last, city = padlist(['George', 'Costanza'], 3)
        ('George', 'Costanza', None)
        >>> first, last, city, planet = padlist(['George', 'Costanza',
                                                 'NYC'], 4, default='Earth')
        ('George', 'Costanza', 'NYC', 'Earth')

    """
    return list(container)[:size] + [default] * (size - len(container))


def mattrgetter(*attrs):
    """Like :func:`operator.itemgetter` but returns :const:`None` on missing
    attributes instead of raising :exc:`AttributeError`."""
    return lambda obj: dict((attr, getattr(obj, attr, None))
                            for attr in attrs)


def uniq(it):
    """Returns all unique elements in ``it``, preserving order."""
    seen = set()
    return (seen.add(obj) or obj for obj in it if obj not in seen)


def regen(it):
    """Regen takes any iterable, and if the object is an
    generator it will cache the evaluated list on first access,
    so that the generator can be "consumed" multiple times."""
    if isinstance(it, (list, tuple)):
        return it
    return _regen(it)


class _regen(UserList, list):
    # must be subclass of list so that json can encode.
    def __init__(self, it):
        self.__it = it

    @cached_property
    def data(self):
        return list(self.__it)


def dictfilter(d, **filterkeys):
    d = dict(d, **filterkeys) if filterkeys else d
    return dict((k, v) for k, v in items(d) if v is not None)
