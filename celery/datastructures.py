# -*- coding: utf-8 -*-
"""
    ``celery.datastructures``
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    Custom types and data-structures.

"""
from __future__ import absolute_import, print_function, unicode_literals

import sys
import time

from collections import (
    Callable, Mapping, MutableMapping, MutableSet, defaultdict,
)
from heapq import heapify, heappush, heappop
from itertools import chain

from billiard.einfo import ExceptionInfo  # noqa
from kombu.utils.encoding import safe_str, bytes_to_str
from kombu.utils.limits import TokenBucket  # noqa

from celery.five import items, python_2_unicode_compatible, values
from celery.utils.functional import LRUCache, first, uniq  # noqa
from celery.utils.text import match_case

try:
    from django.utils.functional import LazyObject, LazySettings
except ImportError:
    class LazyObject(object):  # noqa
        pass
    LazySettings = LazyObject  # noqa

__all__ = [
    'GraphFormatter', 'CycleError', 'DependencyGraph',
    'AttributeDictMixin', 'AttributeDict', 'DictAttribute',
    'ConfigurationView', 'LimitedSet',
]

DOT_HEAD = """
{IN}{type} {id} {{
{INp}graph [{attrs}]
"""
DOT_ATTR = '{name}={value}'
DOT_NODE = '{INp}"{0}" [{attrs}]'
DOT_EDGE = '{INp}"{0}" {dir} "{1}" [{attrs}]'
DOT_ATTRSEP = ', '
DOT_DIRS = {'graph': '--', 'digraph': '->'}
DOT_TAIL = '{IN}}}'

REPR_LIMITED_SET = """\
<{name}({size}): maxlen={0.maxlen}, expires={0.expires}, minlen={0.minlen}>\
"""


def force_mapping(m):
    if isinstance(m, (LazyObject, LazySettings)):
        m = m._wrapped
    return DictAttribute(m) if not isinstance(m, Mapping) else m


class GraphFormatter(object):
    _attr = DOT_ATTR.strip()
    _node = DOT_NODE.strip()
    _edge = DOT_EDGE.strip()
    _head = DOT_HEAD.strip()
    _tail = DOT_TAIL.strip()
    _attrsep = DOT_ATTRSEP
    _dirs = dict(DOT_DIRS)

    scheme = {
        'shape': 'box',
        'arrowhead': 'vee',
        'style': 'filled',
        'fontname': 'HelveticaNeue',
    }
    edge_scheme = {
        'color': 'darkseagreen4',
        'arrowcolor': 'black',
        'arrowsize': 0.7,
    }
    node_scheme = {'fillcolor': 'palegreen3', 'color': 'palegreen4'}
    term_scheme = {'fillcolor': 'palegreen1', 'color': 'palegreen2'}
    graph_scheme = {'bgcolor': 'mintcream'}

    def __init__(self, root=None, type=None, id=None,
                 indent=0, inw=' ' * 4, **scheme):
        self.id = id or 'dependencies'
        self.root = root
        self.type = type or 'digraph'
        self.direction = self._dirs[self.type]
        self.IN = inw * (indent or 0)
        self.INp = self.IN + inw
        self.scheme = dict(self.scheme, **scheme)
        self.graph_scheme = dict(self.graph_scheme, root=self.label(self.root))

    def attr(self, name, value):
        value = '"{0}"'.format(value)
        return self.FMT(self._attr, name=name, value=value)

    def attrs(self, d, scheme=None):
        d = dict(self.scheme, **dict(scheme, **d or {}) if scheme else d)
        return self._attrsep.join(
            safe_str(self.attr(k, v)) for k, v in items(d)
        )

    def head(self, **attrs):
        return self.FMT(
            self._head, id=self.id, type=self.type,
            attrs=self.attrs(attrs, self.graph_scheme),
        )

    def tail(self):
        return self.FMT(self._tail)

    def label(self, obj):
        return obj

    def node(self, obj, **attrs):
        return self.draw_node(obj, self.node_scheme, attrs)

    def terminal_node(self, obj, **attrs):
        return self.draw_node(obj, self.term_scheme, attrs)

    def edge(self, a, b, **attrs):
        return self.draw_edge(a, b, **attrs)

    def _enc(self, s):
        return s.encode('utf-8', 'ignore')

    def FMT(self, fmt, *args, **kwargs):
        return self._enc(fmt.format(
            *args, **dict(kwargs, IN=self.IN, INp=self.INp)
        ))

    def draw_edge(self, a, b, scheme=None, attrs=None):
        return self.FMT(
            self._edge, self.label(a), self.label(b),
            dir=self.direction, attrs=self.attrs(attrs, self.edge_scheme),
        )

    def draw_node(self, obj, scheme=None, attrs=None):
        return self.FMT(
            self._node, self.label(obj), attrs=self.attrs(attrs, scheme),
        )


class CycleError(Exception):
    """A cycle was detected in an acyclic graph."""


@python_2_unicode_compatible
class DependencyGraph(object):
    """A directed acyclic graph of objects and their dependencies.

    Supports a robust topological sort
    to detect the order in which they must be handled.

    Takes an optional iterator of ``(obj, dependencies)``
    tuples to build the graph from.

    .. warning::

        Does not support cycle detection.

    """

    def __init__(self, it=None, formatter=None):
        self.formatter = formatter or GraphFormatter()
        self.adjacent = {}
        if it is not None:
            self.update(it)

    def add_arc(self, obj):
        """Add an object to the graph."""
        self.adjacent.setdefault(obj, [])

    def add_edge(self, A, B):
        """Add an edge from object ``A`` to object ``B``
        (``A`` depends on ``B``)."""
        self[A].append(B)

    def connect(self, graph):
        """Add nodes from another graph."""
        self.adjacent.update(graph.adjacent)

    def topsort(self):
        """Sort the graph topologically.

        :returns: a list of objects in the order
            in which they must be handled.

        """
        graph = DependencyGraph()
        components = self._tarjan72()

        NC = {
            node: component for component in components for node in component
        }
        for component in components:
            graph.add_arc(component)
        for node in self:
            node_c = NC[node]
            for successor in self[node]:
                successor_c = NC[successor]
                if node_c != successor_c:
                    graph.add_edge(node_c, successor_c)
        return [t[0] for t in graph._khan62()]

    def valency_of(self, obj):
        """Return the valency (degree) of a vertex in the graph."""
        try:
            l = [len(self[obj])]
        except KeyError:
            return 0
        for node in self[obj]:
            l.append(self.valency_of(node))
        return sum(l)

    def update(self, it):
        """Update the graph with data from a list
        of ``(obj, dependencies)`` tuples."""
        tups = list(it)
        for obj, _ in tups:
            self.add_arc(obj)
        for obj, deps in tups:
            for dep in deps:
                self.add_edge(obj, dep)

    def edges(self):
        """Return generator that yields for all edges in the graph."""
        return (obj for obj, adj in items(self) if adj)

    def _khan62(self):
        """Khans simple topological sort algorithm from '62

        See https://en.wikipedia.org/wiki/Topological_sorting

        """
        count = defaultdict(lambda: 0)
        result = []

        for node in self:
            for successor in self[node]:
                count[successor] += 1
        ready = [node for node in self if not count[node]]

        while ready:
            node = ready.pop()
            result.append(node)

            for successor in self[node]:
                count[successor] -= 1
                if count[successor] == 0:
                    ready.append(successor)
        result.reverse()
        return result

    def _tarjan72(self):
        """Tarjan's algorithm to find strongly connected components.

        See http://bit.ly/vIMv3h.

        """
        result, stack, low = [], [], {}

        def visit(node):
            if node in low:
                return
            num = len(low)
            low[node] = num
            stack_pos = len(stack)
            stack.append(node)

            for successor in self[node]:
                visit(successor)
                low[node] = min(low[node], low[successor])

            if num == low[node]:
                component = tuple(stack[stack_pos:])
                stack[stack_pos:] = []
                result.append(component)
                for item in component:
                    low[item] = len(self)

        for node in self:
            visit(node)

        return result

    def to_dot(self, fh, formatter=None):
        """Convert the graph to DOT format.

        :param fh: A file, or a file-like object to write the graph to.

        """
        seen = set()
        draw = formatter or self.formatter

        def P(s):
            print(bytes_to_str(s), file=fh)

        def if_not_seen(fun, obj):
            if draw.label(obj) not in seen:
                P(fun(obj))
                seen.add(draw.label(obj))

        P(draw.head())
        for obj, adjacent in items(self):
            if not adjacent:
                if_not_seen(draw.terminal_node, obj)
            for req in adjacent:
                if_not_seen(draw.node, obj)
                P(draw.edge(obj, req))
        P(draw.tail())

    def format(self, obj):
        return self.formatter(obj) if self.formatter else obj

    def __iter__(self):
        return iter(self.adjacent)

    def __getitem__(self, node):
        return self.adjacent[node]

    def __len__(self):
        return len(self.adjacent)

    def __contains__(self, obj):
        return obj in self.adjacent

    def _iterate_items(self):
        return items(self.adjacent)
    items = iteritems = _iterate_items

    def __repr__(self):
        return '\n'.join(self.repr_node(N) for N in self)

    def repr_node(self, obj, level=1, fmt='{0}({1})'):
        output = [fmt.format(obj, self.valency_of(obj))]
        if obj in self:
            for other in self[obj]:
                d = fmt.format(other, self.valency_of(other))
                output.append('     ' * level + d)
                output.extend(self.repr_node(other, level + 1).split('\n')[1:])
        return '\n'.join(output)


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


@python_2_unicode_compatible
class ConfigurationView(AttributeDictMixin):
    """A view over an applications configuration dictionaries.

    Custom (but older) version of :class:`collections.ChainMap`.

    If the key does not exist in ``changes``, the ``defaults``
    dictionaries are consulted.

    :param changes:  Dict containing changes to the configuration.
    :param defaults: List of dictionaries containing the default
                     configuration.

    """
    key_t = None
    changes = None
    defaults = None
    _order = None

    def __init__(self, changes, defaults=None, key_t=None, prefix=None):
        defaults = [] if defaults is None else defaults
        self.__dict__.update(
            changes=changes,
            defaults=defaults,
            key_t=key_t,
            _order=[changes] + defaults,
            prefix=prefix.rstrip('_') + '_' if prefix else prefix,
        )

    def _to_keys(self, key):
        prefix = self.prefix
        if prefix:
            pkey = prefix + key if not key.startswith(prefix) else key
            return match_case(pkey, prefix), self._key(key)
        return self._key(key),

    def _key(self, key):
        return self.key_t(key) if self.key_t is not None else key

    def add_defaults(self, d):
        d = force_mapping(d)
        self.defaults.insert(0, d)
        self._order.insert(1, d)

    def __getitem__(self, key):
        keys = self._to_keys(key)
        for k in keys:
            for d in self._order:
                try:
                    return d[k]
                except KeyError:
                    pass
        if len(keys) > 1:
            raise KeyError(
                'Key not found: {0!r} (with prefix: {0!r})'.format(*keys))
        raise KeyError(key)

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

    def setdefault(self, key, default):
        key = self._key(key)
        if key not in self:
            self[key] = default

    def update(self, *args, **kwargs):
        return self.changes.update(*args, **kwargs)

    def __contains__(self, key):
        keys = self._to_keys(key)
        return any(any(k in m for k in keys) for m in self._order)

    def __bool__(self):
        return any(self._order)
    __nonzero__ = __bool__  # Py2

    def __repr__(self):
        return repr(dict(items(self)))

    def __iter__(self):
        return self._iterate_keys()

    def __len__(self):
        # The logic for iterating keys includes uniq(),
        # so to be safe we count by explicitly iterating
        return len(set().union(*self._order))

    def _iter(self, op):
        # defaults must be first in the stream, so values in
        # changes takes precedence.
        return chain(*[op(d) for d in reversed(self._order)])

    def swap_with(self, other):
        changes = other.__dict__['changes']
        defaults = other.__dict__['defaults']
        self.__dict__.update(
            changes=changes,
            defaults=defaults,
            key_t=other.__dict__['key_t'],
            prefix=other.__dict__['prefix'],
            _order=[changes] + defaults
        )

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
MutableMapping.register(ConfigurationView)


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
