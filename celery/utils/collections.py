# -*- coding: utf-8 -*-
"""
    ``celery.utils.collections``
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Custom maps, sets, sequences and other data structures.

"""
from __future__ import absolute_import, unicode_literals

import sys
import time

from collections import (
    Callable, Mapping, MutableMapping, MutableSet, Sequence,
    OrderedDict as _OrderedDict, deque,
)
from heapq import heapify, heappush, heappop
from itertools import chain, count

from celery.five import Empty, items, keys, python_2_unicode_compatible, values

from .functional import first, uniq
from .text import match_case

try:
    # pypy: dicts are ordered in recent versions
    from __pypy__ import reversed_dict as _dict_is_ordered
except ImportError:
    _dict_is_ordered = None

try:
    from django.utils.functional import LazyObject, LazySettings
except ImportError:
    class LazyObject(object):  # noqa
        pass
    LazySettings = LazyObject  # noqa

__all__ = [
    'AttributeDictMixin', 'AttributeDict', 'BufferMap', 'ChainMap',
    'ConfigurationView', 'DictAttribute', 'Evictable',
    'LimitedSet', 'Messagebuffer', 'OrderedDict',
    'force_mapping', 'lpmerge',
]

PY3 = sys.version_info[0] >= 3

REPR_LIMITED_SET = """\
<{name}({size}): maxlen={0.maxlen}, expires={0.expires}, minlen={0.minlen}>\
"""


def force_mapping(m):
    if isinstance(m, (LazyObject, LazySettings)):
        m = m._wrapped
    return DictAttribute(m) if not isinstance(m, Mapping) else m


def lpmerge(L, R):
    """In place left precedent dictionary merge.

    Keeps values from `L`, if the value in `R` is :const:`None`."""
    setitem = L.__setitem__
    [setitem(k, v) for k, v in items(R) if v is not None]
    return L


class OrderedDict(_OrderedDict):

    if PY3:  # pragma: no cover
        def _LRUkey(self):
            # return value of od.keys does not support __next__,
            # but this version will also not create a copy of the list.
            return next(iter(keys(self)))
    else:
        if _dict_is_ordered:  # pragma: no cover
            def _LRUkey(self):
                # iterkeys is iterable.
                return next(self.iterkeys())
        else:
            def _LRUkey(self):
                return self._OrderedDict__root[1][2]

    if not hasattr(_OrderedDict, 'move_to_end'):
        if _dict_is_ordered:  # pragma: no cover

            def move_to_end(self, key, last=True):
                if not last:
                    # we don't use this argument, and the only way to
                    # implement this on PyPy seems to be O(n): creating a
                    # copy with the order changed, so we just raise.
                    raise NotImplementedError('no last=True on PyPy')
                self[key] = self.pop(key)

        else:

            def move_to_end(self, key, last=True):
                link = self._OrderedDict__map[key]
                link_prev = link[0]
                link_next = link[1]
                link_prev[1] = link_next
                link_next[0] = link_prev
                root = self._OrderedDict__root
                if last:
                    last = root[0]
                    link[0] = last
                    link[1] = root
                    last[1] = root[0] = link
                else:
                    first = root[1]
                    link[0] = root
                    link[1] = first
                    root[1] = first[0] = link


class AttributeDictMixin(object):
    """Augment classes with a Mapping interface by adding attribute access.

    I.e. `d.key -> d[key]`.

    """

    def __getattr__(self, k):
        """`d.key -> d[key]`"""
        try:
            return self[k]
        except KeyError:
            raise AttributeError(
                '{0!r} object has no attribute {1!r}'.format(
                    type(self).__name__, k))

    def __setattr__(self, key, value):
        """`d[key] = value -> d.key = value`"""
        self[key] = value


class AttributeDict(dict, AttributeDictMixin):
    """Dict subclass with attribute access."""
    pass


class DictAttribute(object):
    """Dict interface to attributes.

    `obj[k] -> obj.k`
    `obj[k] = val -> obj.k = val`

    """
    obj = None

    def __init__(self, obj):
        object.__setattr__(self, 'obj', obj)

    def __getattr__(self, key):
        return getattr(self.obj, key)

    def __setattr__(self, key, value):
        return setattr(self.obj, key, value)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def setdefault(self, key, default):
        if key not in self:
            self[key] = default

    def __getitem__(self, key):
        try:
            return getattr(self.obj, key)
        except AttributeError:
            raise KeyError(key)

    def __setitem__(self, key, value):
        setattr(self.obj, key, value)

    def __contains__(self, key):
        return hasattr(self.obj, key)

    def _iterate_keys(self):
        return iter(dir(self.obj))
    iterkeys = _iterate_keys

    def __iter__(self):
        return self._iterate_keys()

    def _iterate_items(self):
        for key in self._iterate_keys():
            yield key, getattr(self.obj, key)
    iteritems = _iterate_items

    def _iterate_values(self):
        for key in self._iterate_keys():
            yield getattr(self.obj, key)
    itervalues = _iterate_values

    if sys.version_info[0] == 3:  # pragma: no cover
        items = _iterate_items
        keys = _iterate_keys
        values = _iterate_values
    else:

        def keys(self):
            return list(self)

        def items(self):
            return list(self._iterate_items())

        def values(self):
            return list(self._iterate_values())
MutableMapping.register(DictAttribute)


class ChainMap(MutableMapping):

    key_t = None
    changes = None
    defaults = None
    maps = None

    def __init__(self, *maps, **kwargs):
        maps = list(maps or [{}])
        self.__dict__.update(
            key_t=kwargs.get('key_t'),
            maps=maps,
            changes=maps[0],
            defaults=maps[1:],
        )

    def add_defaults(self, d):
        d = force_mapping(d)
        self.defaults.insert(0, d)
        self.maps.insert(1, d)

    def pop(self, key, *default):
        try:
            return self.maps[0].pop(key, *default)
        except KeyError:
            raise KeyError(
                'Key not found in the first mapping: {!r}'.format(key))

    def __missing__(self, key):
        raise KeyError(key)

    def _key(self, key):
        return self.key_t(key) if self.key_t is not None else key

    def __getitem__(self, key):
        _key = self._key(key)
        for mapping in self.maps:
            try:
                return mapping[_key]
            except KeyError:
                pass
        return self.__missing__(key)

    def __setitem__(self, key, value):
        self.changes[self._key(key)] = value

    def __delitem__(self, key):
        try:
            del self.changes[self._key(key)]
        except KeyError:
            raise KeyError('Key not found in first mapping: {0!r}'.format(key))

    def clear(self):
        self.changes.clear()

    def get(self, key, default=None):
        try:
            return self[self._key(key)]
        except KeyError:
            return default

    def __len__(self):
        return len(set().union(*self.maps))

    def __iter__(self):
        return self._iterate_keys()

    def __contains__(self, key):
        key = self._key(key)
        return any(key in m for m in self.maps)

    def __bool__(self):
        return any(self.maps)
    __nonzero__ = __bool__  # Py2

    def setdefault(self, key, default):
        key = self._key(key)
        if key not in self:
            self[key] = default

    def update(self, *args, **kwargs):
        return self.changes.update(*args, **kwargs)

    def __repr__(self):
        return '{0.__class__.__name__}({1})'.format(
            self, ', '.join(map(repr, self.maps)))

    @classmethod
    def fromkeys(cls, iterable, *args):
        """Create a ChainMap with a single dict created from the iterable."""
        return cls(dict.fromkeys(iterable, *args))

    def copy(self):
        """New ChainMap or subclass with a new copy of maps[0] and
        refs to maps[1:]."""
        return self.__class__(self.maps[0].copy(), *self.maps[1:])
    __copy__ = copy  # Py2

    def _iter(self, op):
        # defaults must be first in the stream, so values in
        # changes take precedence.
        return chain(*[op(d) for d in reversed(self.maps)])

    def _iterate_keys(self):
        return uniq(self._iter(lambda d: d.keys()))
    iterkeys = _iterate_keys

    def _iterate_items(self):
        return ((key, self[key]) for key in self)
    iteritems = _iterate_items

    def _iterate_values(self):
        return (self[key] for key in self)
    itervalues = _iterate_values

    if sys.version_info[0] == 3:  # pragma: no cover
        keys = _iterate_keys
        items = _iterate_items
        values = _iterate_values

    else:  # noqa
        def keys(self):
            return list(self._iterate_keys())

        def items(self):
            return list(self._iterate_items())

        def values(self):
            return list(self._iterate_values())


@python_2_unicode_compatible
class ConfigurationView(ChainMap, AttributeDictMixin):
    """A view over an applications configuration dictionaries.

    Custom (but older) version of :class:`collections.ChainMap`.

    If the key does not exist in ``changes``, the ``defaults``
    dictionaries are consulted.

    :param changes:  Dict containing changes to the configuration.
    :param defaults: List of dictionaries containing the default
                     configuration.

    """

    def __init__(self, changes, defaults=None, key_t=None, prefix=None):
        defaults = [] if defaults is None else defaults
        super(ConfigurationView, self).__init__(
            changes, *defaults, **{'key_t': key_t})
        self.__dict__.update(
            prefix=prefix.rstrip('_') + '_' if prefix else prefix,
        )

    def _to_keys(self, key):
        prefix = self.prefix
        if prefix:
            pkey = prefix + key if not key.startswith(prefix) else key
            return match_case(pkey, prefix), key
        return key,

    def __getitem__(self, key):
        keys = self._to_keys(key)
        getitem = super(ConfigurationView, self).__getitem__
        for k in keys:
            try:
                return getitem(k)
            except KeyError:
                pass
        try:
            # support subclasses implementing __missing__
            return self.__missing__(key)
        except KeyError:
            if len(keys) > 1:
                raise KeyError(
                    'Key not found: {0!r} (with prefix: {0!r})'.format(*keys))
            raise

    def __setitem__(self, key, value):
        self.changes[self._key(key)] = value

    def first(self, *keys):
        return first(None, (self.get(key) for key in keys))

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def clear(self):
        """Remove all changes, but keep defaults."""
        self.changes.clear()

    def __contains__(self, key):
        keys = self._to_keys(key)
        return any(any(k in m for k in keys) for m in self.maps)

    def swap_with(self, other):
        changes = other.__dict__['changes']
        defaults = other.__dict__['defaults']
        self.__dict__.update(
            changes=changes,
            defaults=defaults,
            key_t=other.__dict__['key_t'],
            prefix=other.__dict__['prefix'],
            maps=[changes] + defaults
        )


@python_2_unicode_compatible
class LimitedSet(object):
    """Kind-of Set (or priority queue) with limitations.

    Good for when you need to test for membership (`a in set`),
    but the set should not grow unbounded.

    ``maxlen`` is enforced at all times, so if the limit is reached
    we will also remove non-expired items.

    You can also configure ``minlen``, which is the minimal residual size
    of the set.

    All arguments are optional, and no limits are enabled by default.

    :keyword maxlen: Optional max number of items.

        Adding more items than ``maxlen`` will result in immediate
        removal of items sorted by oldest insertion time.

    :keyword expires: TTL for all items.

        Expired items are purged as keys are inserted.

    :keyword minlen: Minimal residual size of this set.

        .. versionadded:: 4.0

        Value must be less than ``maxlen`` if both are configured.

        Older expired items will be deleted, only after the set
        exceeds ``minlen`` number of items.

    :keyword data: Initial data to initialize set with.
        Can be an iterable of ``(key, value)`` pairs,
        a dict (``{key: insertion_time}``), or another instance
        of :class:`LimitedSet`.

    Example:

    .. code-block:: pycon

        >>> s = LimitedSet(maxlen=50000, expires=3600, minlen=4000)
        >>> for i in range(60000):
        ...     s.add(i)
        ...     s.add(str(i))
        ...
        >>> 57000 in s  # last 50k inserted values are kept
        True
        >>> '10' in s  # '10' did expire and was purged from set.
        False
        >>> len(s)  # maxlen is reached
        50000
        >>> s.purge(now=time.time() + 7200)  # clock + 2 hours
        >>> len(s)  # now only minlen items are cached
        4000
        >>>> 57000 in s  # even this item is gone now
        False

    """

    max_heap_percent_overload = 15

    def __init__(self, maxlen=0, expires=0, data=None, minlen=0):
        self.maxlen = 0 if maxlen is None else maxlen
        self.minlen = 0 if minlen is None else minlen
        self.expires = 0 if expires is None else expires
        self._data = {}
        self._heap = []

        # make shortcuts
        self.__len__ = self._data.__len__
        self.__contains__ = self._data.__contains__

        if data:
            # import items from data
            self.update(data)

        if not self.maxlen >= self.minlen >= 0:
            raise ValueError(
                'minlen must be a positive number, less or equal to maxlen.')
        if self.expires < 0:
            raise ValueError('expires cannot be negative!')

    def _refresh_heap(self):
        """Time consuming recreating of heap. Do not run this too often."""
        self._heap[:] = [entry for entry in values(self._data)]
        heapify(self._heap)

    def _maybe_refresh_heap(self):
        if self._heap_overload >= self.max_heap_percent_overload:
            self._refresh_heap()

    def clear(self):
        """Clear all data, start from scratch again."""
        self._data.clear()
        self._heap[:] = []

    def add(self, item, now=None):
        """Add a new item, or reset the expiry time of an existing item."""
        now = now or time.time()
        if item in self._data:
            self.discard(item)
        entry = (now, item)
        self._data[item] = entry
        heappush(self._heap, entry)
        if self.maxlen and len(self._data) >= self.maxlen:
            self.purge()

    def update(self, other):
        """Update this set from other LimitedSet, dict or iterable."""
        if not other:
            return
        if isinstance(other, LimitedSet):
            self._data.update(other._data)
            self._refresh_heap()
            self.purge()
        elif isinstance(other, dict):
            # revokes are sent as a dict
            for key, inserted in items(other):
                if isinstance(inserted, (tuple, list)):
                    # in case someone uses ._data directly for sending update
                    inserted = inserted[0]
                if not isinstance(inserted, float):
                    raise ValueError(
                        'Expecting float timestamp, got type '
                        '{0!r} with value: {1}'.format(
                            type(inserted), inserted))
                self.add(key, inserted)
        else:
            # XXX AVOID THIS, it could keep old data if more parties
            # exchange them all over and over again
            for obj in other:
                self.add(obj)

    def discard(self, item):
        # mark an existing item as removed. If KeyError is not found, pass.
        self._data.pop(item, None)
        self._maybe_refresh_heap()
    pop_value = discard

    def purge(self, now=None):
        """Check oldest items and remove them if needed.

        :keyword now: Time of purging -- by default right now.
                      This can be useful for unit testing.

        """
        now = now or time.time()
        now = now() if isinstance(now, Callable) else now
        if self.maxlen:
            while len(self._data) > self.maxlen:
                self.pop()
        # time based expiring:
        if self.expires:
            while len(self._data) > self.minlen >= 0:
                inserted_time, _ = self._heap[0]
                if inserted_time + self.expires > now:
                    break  # oldest item has not expired yet
                self.pop()

    def pop(self, default=None):
        """Remove and return the oldest item, or :const:`None` when empty."""
        while self._heap:
            _, item = heappop(self._heap)
            try:
                self._data.pop(item)
            except KeyError:
                pass
            else:
                return item
        return default

    def as_dict(self):
        """Whole set as serializable dictionary.

        Example::

            >>> s = LimitedSet(maxlen=200)
            >>> r = LimitedSet(maxlen=200)
            >>> for i in range(500):
            ...     s.add(i)
            ...
            >>> r.update(s.as_dict())
            >>> r == s
            True

        """
        return {key: inserted for inserted, key in values(self._data)}

    def __eq__(self, other):
        return self._data == other._data

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return REPR_LIMITED_SET.format(
            self, name=type(self).__name__, size=len(self),
        )

    def __iter__(self):
        return (i for _, i in sorted(values(self._data)))

    def __len__(self):
        return len(self._data)

    def __contains__(self, key):
        return key in self._data

    def __reduce__(self):
        return self.__class__, (
            self.maxlen, self.expires, self.as_dict(), self.minlen)

    def __bool__(self):
        return bool(self._data)
    __nonzero__ = __bool__  # Py2

    @property
    def _heap_overload(self):
        """Compute how much is heap bigger than data [percents]."""
        return len(self._heap) * 100 / max(len(self._data), 1) - 100
MutableSet.register(LimitedSet)


class Evictable(object):

    Empty = Empty

    def evict(self):
        """Force evict until maxsize is enforced."""
        self._evict(range=count)

    def _evict(self, limit=100, range=range):
        try:
            [self._evict1() for _ in range(limit)]
        except IndexError:
            pass

    def _evict1(self):
        if self._evictcount <= self.maxsize:
            raise IndexError()
        try:
            self._pop_to_evict()
        except self.Empty:
            raise IndexError()


@python_2_unicode_compatible
class Messagebuffer(Evictable):

    Empty = Empty

    def __init__(self, maxsize, iterable=None, deque=deque):
        self.maxsize = maxsize
        self.data = deque(iterable or [])
        self._append = self.data.append
        self._pop = self.data.popleft
        self._len = self.data.__len__
        self._extend = self.data.extend

    def put(self, item):
        self._append(item)
        self.maxsize and self._evict()

    def extend(self, it):
        self._extend(it)
        self.maxsize and self._evict()

    def take(self, *default):
        try:
            return self._pop()
        except IndexError:
            if default:
                return default[0]
            raise self.Empty()

    def _pop_to_evict(self):
        return self.take()

    def __repr__(self):
        return '<{0}: {1}/{2}>'.format(
            type(self).__name__, len(self), self.maxsize,
        )

    def __iter__(self):
        while 1:
            try:
                yield self._pop()
            except IndexError:
                break

    def __len__(self):
        return self._len()

    def __contains__(self, item):
        return item in self.data

    def __reversed__(self):
        return reversed(self.data)

    def __getitem__(self, index):
        return self.data[index]

    @property
    def _evictcount(self):
        return len(self)
Sequence.register(Messagebuffer)


@python_2_unicode_compatible
class BufferMap(OrderedDict, Evictable):

    Buffer = Messagebuffer
    Empty = Empty

    maxsize = None
    total = 0
    bufmaxsize = None

    def __init__(self, maxsize, iterable=None, bufmaxsize=1000):
        super(BufferMap, self).__init__()
        self.maxsize = maxsize
        self.bufmaxsize = 1000
        if iterable:
            self.update(iterable)
        self.total = sum(len(buf) for buf in items(self))

    def put(self, key, item):
        self._get_or_create_buffer(key).put(item)
        self.total += 1
        self.move_to_end(key)   # least recently used.
        self.maxsize and self._evict()

    def extend(self, key, it):
        self._get_or_create_buffer(key).extend(it)
        self.total += len(it)
        self.maxsize and self._evict()

    def take(self, key, *default):
        item, throw = None, False
        try:
            buf = self[key]
        except KeyError:
            throw = True
        else:
            try:
                item = buf.take()
                self.total -= 1
            except self.Empty:
                throw = True
            else:
                self.move_to_end(key)  # mark as LRU

        if throw:
            if default:
                return default[0]
            raise self.Empty()
        return item

    def _get_or_create_buffer(self, key):
        try:
            return self[key]
        except KeyError:
            buf = self[key] = self._new_buffer()
            return buf

    def _new_buffer(self):
        return self.Buffer(maxsize=self.bufmaxsize)

    def _LRUpop(self, *default):
        return self[self._LRUkey()].take(*default)

    def _pop_to_evict(self):
        for i in range(100):
            key = self._LRUkey()
            buf = self[key]
            try:
                buf.take()
            except (IndexError, self.Empty):
                # buffer empty, remove it from mapping.
                self.pop(key)
            else:
                # we removed one item
                self.total -= 1
                # if buffer is empty now, remove it from mapping.
                if not len(buf):
                    self.pop(key)
                else:
                    # move to least recently used.
                    self.move_to_end(key)
                break

    def __repr__(self):
        return '<{0}: {1}/{2}>'.format(
            type(self).__name__, self.total, self.maxsize,
        )

    @property
    def _evictcount(self):
        return self.total
