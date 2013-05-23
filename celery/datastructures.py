# -*- coding: utf-8 -*-
"""
    celery.datastructures
    ~~~~~~~~~~~~~~~~~~~~~

    Custom types and data structures.

"""
from __future__ import absolute_import
from __future__ import with_statement

import sys
import time

from collections import defaultdict
from heapq import heapify, heappush, heappop
from itertools import chain

try:
    from collections import Mapping, MutableMapping
except ImportError:        # pragma: no cover
    MutableMapping = None  # noqa
    Mapping = dict         # noqa

from billiard.einfo import ExceptionInfo  # noqa
from kombu.utils.limits import TokenBucket  # noqa

from .utils.functional import LRUCache, first, uniq  # noqa


class CycleError(Exception):
    """A cycle was detected in an acyclic graph."""


class DependencyGraph(object):
    """A directed acyclic graph of objects and their dependencies.

    Supports a robust topological sort
    to detect the order in which they must be handled.

    Takes an optional iterator of ``(obj, dependencies)``
    tuples to build the graph from.

    .. warning::

        Does not support cycle detection.

    """

    def __init__(self, it=None):
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

    def topsort(self):
        """Sort the graph topologically.

        :returns: a list of objects in the order
            in which they must be handled.

        """
        graph = DependencyGraph()
        components = self._tarjan72()

        NC = dict((node, component)
                  for component in components
                  for node in component)
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
        """Returns the velency (degree) of a vertex in the graph."""
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
        """Returns generator that yields for all edges in the graph."""
        return (obj for obj, adj in self.iteritems() if adj)

    def _khan62(self):
        """Khans simple topological sort algorithm from '62

        See http://en.wikipedia.org/wiki/Topological_sorting

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

    def to_dot(self, fh, ws=' ' * 4):
        """Convert the graph to DOT format.

        :param fh: A file, or a file-like object to write the graph to.

        """
        fh.write('digraph dependencies {\n')
        for obj, adjacent in self.iteritems():
            if not adjacent:
                fh.write(ws + '"%s"\n' % (obj, ))
            for req in adjacent:
                fh.write(ws + '"%s" -> "%s"\n' % (obj, req))
        fh.write('}\n')

    def __iter__(self):
        return iter(self.adjacent)

    def __getitem__(self, node):
        return self.adjacent[node]

    def __len__(self):
        return len(self.adjacent)

    def __contains__(self, obj):
        return obj in self.adjacent

    def _iterate_items(self):
        return self.adjacent.iteritems()
    items = iteritems = _iterate_items

    def __repr__(self):
        return '\n'.join(self.repr_node(N) for N in self)

    def repr_node(self, obj, level=1):
        output = ['%s(%s)' % (obj, self.valency_of(obj))]
        if obj in self:
            for other in self[obj]:
                d = '%s(%s)' % (other, self.valency_of(other))
                output.append('     ' * level + d)
                output.extend(self.repr_node(other, level + 1).split('\n')[1:])
        return '\n'.join(output)


class AttributeDictMixin(object):
    """Adds attribute access to mappings.

    `d.key -> d[key]`

    """

    def __getattr__(self, k):
        """`d.key -> d[key]`"""
        try:
            return self[k]
        except KeyError:
            raise AttributeError(
                "'%s' object has no attribute '%s'" % (type(self).__name__, k))

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

    def _iterate_keys(self):
        return iter(dir(self.obj))
    iterkeys = _iterate_keys

    def __iter__(self):
        return self._iterate_keys()

    def _iterate_items(self):
        for key in self._iterate_keys():
            yield key, getattr(self.obj, key)
    iteritems = _iterate_items

    if sys.version_info[0] == 3:  # pragma: no cover
        items = _iterate_items
        keys = _iterate_keys
    else:

        def keys(self):
            return list(self)

        def items(self):
            return list(self._iterate_items())


class ConfigurationView(AttributeDictMixin):
    """A view over an applications configuration dicts.

    If the key does not exist in ``changes``, the ``defaults`` dicts
    are consulted.

    :param changes:  Dict containing changes to the configuration.
    :param defaults: List of dicts containing the default configuration.

    """
    changes = None
    defaults = None
    _order = None

    def __init__(self, changes, defaults):
        self.__dict__.update(changes=changes, defaults=defaults,
                             _order=[changes] + defaults)

    def add_defaults(self, d):
        if not isinstance(d, Mapping):
            d = DictAttribute(d)
        self.defaults.insert(0, d)
        self._order.insert(1, d)

    def __getitem__(self, key):
        for d in self._order:
            try:
                return d[key]
            except KeyError:
                pass
        raise KeyError(key)

    def __setitem__(self, key, value):
        self.changes[key] = value

    def first(self, *keys):
        return first(None, (self.get(key) for key in keys))

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
        return self._iterate_keys()

    def __len__(self):
        # The logic for iterating keys includes uniq(),
        # so to be safe we count by explicitly iterating
        return len(self.keys())

    def _iter(self, op):
        # defaults must be first in the stream, so values in
        # changes takes precedence.
        return chain(*[op(d) for d in reversed(self._order)])

    def _iterate_keys(self):
        return uniq(self._iter(lambda d: d))
    iterkeys = _iterate_keys

    def _iterate_items(self):
        return ((key, self[key]) for key in self)
    iteritems = _iterate_items

    def _iterate_values(self):
        return (self[key] for key in self)
    itervalues = _iterate_values

    def keys(self):
        return list(self._iterate_keys())

    def items(self):
        return list(self._iterate_items())

    def values(self):
        return list(self._iterate_values())
if MutableMapping:
    MutableMapping.register(ConfigurationView)


class LimitedSet(object):
    """Kind-of Set with limitations.

    Good for when you need to test for membership (`a in set`),
    but the list might become to big, so you want to limit it so it doesn't
    consume too much resources.

    :keyword maxlen: Maximum number of members before we start
                     evicting expired members.
    :keyword expires: Time in seconds, before a membership expires.

    """
    __slots__ = ('maxlen', 'expires', '_data', '__len__', '_heap')

    def __init__(self, maxlen=None, expires=None, data=None, heap=None):
        self.maxlen = maxlen
        self.expires = expires
        self._data = data or {}
        self._heap = heap or []
        self.__len__ = self._data.__len__

    def add(self, value):
        """Add a new member."""
        self.purge(1)
        now = time.time()
        self._data[value] = now
        heappush(self._heap, (now, value))

    def __reduce__(self):
        return self.__class__, (
            self.maxlen, self.expires, self._data, self._heap,
        )

    def clear(self):
        """Remove all members"""
        self._data.clear()
        self._heap[:] = []

    def pop_value(self, value):
        """Remove membership by finding value."""
        try:
            itime = self._data[value]
        except KeyError:
            return
        try:
            self._heap.remove((value, itime))
        except ValueError:
            pass
        self._data.pop(value, None)

    def _expire_item(self):
        """Hunt down and remove an expired item."""
        self.purge(1)

    def __contains__(self, value):
        return value in self._data

    def purge(self, limit=None):
        H, maxlen = self._heap, self.maxlen
        if not maxlen:
            return
        i = 0
        while len(self) >= maxlen:
            if limit and i > limit:
                break
            try:
                item = heappop(H)
            except IndexError:
                break
            if self.expires:
                if time.time() < item[0] + self.expires:
                    heappush(H, item)
                    break
            try:
                self._data.pop(item[1])
            except KeyError:  # out of sync with heap
                pass
            i += 1

    def update(self, other, heappush=heappush):
        if isinstance(other, self.__class__):
            self._data.update(other._data)
            self._heap.extend(other._heap)
            heapify(self._heap)
        else:
            for obj in other:
                self.add(obj)

    def as_dict(self):
        return self._data

    def __iter__(self):
        return iter(self._data)

    def __repr__(self):
        return 'LimitedSet(%s)' % (repr(list(self._data))[:100], )

    @property
    def chronologically(self):
        return [value for _, value in self._heap]

    @property
    def first(self):
        """Get the oldest member."""
        return self._heap[0][1]
