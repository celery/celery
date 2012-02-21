# -*- coding: utf-8 -*-
"""
    celery.datastructures
    ~~~~~~~~~~~~~~~~~~~~~

    Custom types and data structures.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import sys
import time
import traceback

from collections import defaultdict
from itertools import chain
from threading import RLock

from kombu.utils.limits import TokenBucket  # noqa

from .utils import uniq
from .utils.compat import UserDict, OrderedDict


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
        self.adjacent[obj] = []

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
        l = [len(self[obj])]
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

    def to_dot(self, fh, ws=" " * 4):
        """Convert the graph to DOT format.

        :param fh: A file, or a file-like object to write the graph to.

        """
        fh.write("digraph dependencies {\n")
        for obj, adjacent in self.iteritems():
            if not adjacent:
                fh.write(ws + '"%s"\n' % (obj, ))
            for req in adjacent:
                fh.write(ws + '"%s" -> "%s"\n' % (obj, req))
        fh.write("}\n")

    def __iter__(self):
        return self.adjacent.iterkeys()

    def __getitem__(self, node):
        return self.adjacent[node]

    def __len__(self):
        return len(self.adjacent)

    def _iterate_items(self):
        return self.adjacent.iteritems()
    items = iteritems = _iterate_items

    def __repr__(self):
        return '\n'.join(self.repr_node(N) for N in self)

    def repr_node(self, obj, level=1):
        output = ["%s(%s)" % (obj, self.valency_of(obj))]
        for other in self[obj]:
            d = "%s(%s)" % (other, self.valency_of(other))
            output.append('     ' * level + d)
            output.extend(self.repr_node(other, level + 1).split('\n')[1:])
        return '\n'.join(output)


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

    if sys.version_info >= (3, 0):  # pragma: no cover
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
        return uniq(self._iter(lambda d: d.iterkeys()))
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


class _Code(object):

    def __init__(self, code):
        self.co_filename = code.co_filename
        self.co_name = code.co_name


class _Frame(object):
    Code = _Code

    def __init__(self, frame):
        self.f_globals = {
            "__file__": frame.f_globals.get("__file__", "__main__"),
            "__name__": frame.f_globals.get("__name__"),
            "__loader__": frame.f_globals.get("__loader__"),
        }
        self.f_locals = fl = {}
        try:
            fl["__traceback_hide__"] = frame.f_locals["__traceback_hide__"]
        except KeyError:
            pass
        self.f_code = self.Code(frame.f_code)
        self.f_lineno = frame.f_lineno


class _Object(object):

    def __init__(self, **kw):
        [setattr(self, k, v) for k, v in kw.iteritems()]


class _Truncated(object):

    def __init__(self):
        self.tb_lineno = -1
        self.tb_frame = _Object(
                f_globals={"__file__": "",
                           "__name__": "",
                           "__loader__": None},
                f_fileno=None,
                f_code=_Object(co_filename="...",
                               co_name="[rest of traceback truncated]"),
        )
        self.tb_next = None


class Traceback(object):
    Frame = _Frame

    tb_frame = tb_lineno = tb_next = None
    max_frames = sys.getrecursionlimit() / 8

    def __init__(self, tb, max_frames=None, depth=0):
        limit = self.max_frames = max_frames or self.max_frames
        self.tb_frame = self.Frame(tb.tb_frame)
        self.tb_lineno = tb.tb_lineno
        if tb.tb_next is not None:
            if depth <= limit:
                self.tb_next = Traceback(tb.tb_next, limit, depth + 1)
            else:
                self.tb_next = _Truncated()


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

    #: Set to true if this is an internal error.
    internal = False

    def __init__(self, exc_info, internal=False):
        self.type, self.exception, tb = exc_info
        self.tb = Traceback(tb)
        self.traceback = ''.join(traceback.format_exception(*exc_info))
        self.internal = internal

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
            for obj in other:
                self.add(obj)

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
        for k in self:
            try:
                yield (k, self.data[k])
            except KeyError:
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
