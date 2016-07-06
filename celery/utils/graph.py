# -*- coding: utf-8 -*-
"""Dependency graph implementation."""
from collections import Counter
from textwrap import dedent
from typing import (
    Any, Dict, MutableSet, MutableSequence,
    Optional, IO, Iterable, Iterator, Sequence, Tuple,
)

from kombu.utils.encoding import safe_str, bytes_to_str

__all__ = ['DOT', 'CycleError', 'DependencyGraph', 'GraphFormatter']


class DOT:
    HEAD = dedent("""
        {IN}{type} {id} {{
        {INp}graph [{attrs}]
    """)
    ATTR = '{name}={value}'
    NODE = '{INp}"{0}" [{attrs}]'
    EDGE = '{INp}"{0}" {dir} "{1}" [{attrs}]'
    ATTRSEP = ', '
    DIRS = {'graph': '--', 'digraph': '->'}
    TAIL = '{IN}}}'


class CycleError(Exception):
    """A cycle was detected in an acyclic graph."""


class DependencyGraph(Iterable):
    """A directed acyclic graph of objects and their dependencies.

    Supports a robust topological sort
    to detect the order in which they must be handled.

    Takes an optional iterator of ``(obj, dependencies)``
    tuples to build the graph from.

    Warning:
        Does not support cycle detection.
    """

    def __init__(self, it: Optional[Iterable]=None,
                 formatter: Optional['GraphFormatter']=None) -> None:
        self.formatter = formatter or GraphFormatter()
        self.adjacent = {}  # type: Dict[Any, Any]
        if it is not None:
            self.update(it)

    def add_arc(self, obj: Any) -> None:
        """Add an object to the graph."""
        self.adjacent.setdefault(obj, [])

    def add_edge(self, A: Any, B: Any) -> None:
        """Add an edge from object ``A`` to object ``B``
        (``A`` depends on ``B``)."""
        self[A].append(B)

    def connect(self, graph: 'DependencyGraph') -> None:
        """Add nodes from another graph."""
        self.adjacent.update(graph.adjacent)

    def topsort(self) -> Sequence[Any]:
        """Sort the graph topologically.

        Returns:
            List: of objects in the order in which they must be handled.
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

    def valency_of(self, obj: Any) -> int:
        """Return the valency (degree) of a vertex in the graph."""
        try:
            l = [len(self[obj])]
        except KeyError:
            return 0
        for node in self[obj]:
            l.append(self.valency_of(node))
        return sum(l)

    def update(self, it: Iterable) -> None:
        """Update the graph with data from a list
        of ``(obj, dependencies)`` tuples."""
        tups = list(it)
        for obj, _ in tups:
            self.add_arc(obj)
        for obj, deps in tups:
            for dep in deps:
                self.add_edge(obj, dep)

    def edges(self) -> Iterator[Any]:
        """Return generator that yields for all edges in the graph."""
        return (obj for obj, adj in self.items() if adj)

    def _khan62(self) -> Sequence[Any]:
        """Khans simple topological sort algorithm from '62

        See https://en.wikipedia.org/wiki/Topological_sorting
        """
        count = Counter()  # type: Counter
        result = []        # type: MutableSequence[Any]

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

    def _tarjan72(self) -> Sequence[Any]:
        """Tarjan's algorithm to find strongly connected components.

        See Also:
            http://bit.ly/vIMv3h.
        """
        result = []  # type: MutableSequence[Any]
        stack = []   # type: MutableSequence[Any]
        low = {}     # type: Dict[Any, Any]

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

    def to_dot(self, fh: IO,
               formatter: Optional['GraphFormatter']=None) -> None:
        """Convert the graph to DOT format.

        Arguments:
            fh (IO): A file, or a file-like object to write the graph to.
            formatter (celery.utils.graph.GraphFormatter): Custom graph
                formatter to use.
        """
        seen = set()  # type: MutableSet
        draw = formatter or self.formatter

        def P(s):
            print(bytes_to_str(s), file=fh)

        def if_not_seen(fun, obj):
            if draw.label(obj) not in seen:
                P(fun(obj))
                seen.add(draw.label(obj))

        P(draw.head())
        for obj, adjacent in self.items():
            if not adjacent:
                if_not_seen(draw.terminal_node, obj)
            for req in adjacent:
                if_not_seen(draw.node, obj)
                P(draw.edge(obj, req))
        P(draw.tail())

    def format(self, obj: Any) -> Any:
        return self.formatter.node(obj) if self.formatter else obj

    def __iter__(self) -> Iterator[Any]:
        return iter(self.adjacent)

    def __getitem__(self, node: Any) -> Any:
        return self.adjacent[node]

    def __len__(self) -> int:
        return len(self.adjacent)

    def __contains__(self, obj: Any) -> bool:
        return obj in self.adjacent

    def items(self) -> Any:
        return self.adjacent.items()

    def __repr__(self) -> str:
        return '\n'.join(self.repr_node(N) for N in self)

    def repr_node(self, obj: Any, level: int=1, fmt: str='{0}({1})') -> str:
        output = [fmt.format(obj, self.valency_of(obj))]
        if obj in self:
            for other in self[obj]:
                d = fmt.format(other, self.valency_of(other))
                output.append('     ' * level + d)
                output.extend(self.repr_node(other, level + 1).split('\n')[1:])
        return '\n'.join(output)


class GraphFormatter:
    _attr = DOT.ATTR.strip()
    _node = DOT.NODE.strip()
    _edge = DOT.EDGE.strip()
    _head = DOT.HEAD.strip()
    _tail = DOT.TAIL.strip()
    _attrsep = DOT.ATTRSEP
    _dirs = dict(DOT.DIRS)

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

    def __init__(self,
                 root: Any=None,
                 type: Optional[str]=None,
                 id: Optional[str]=None,
                 indent: int=0,
                 inw: str=' ' * 4,
                 **scheme) -> None:
        self.id = id or 'dependencies'
        self.root = root
        self.type = type or 'digraph'
        self.direction = self._dirs[self.type]
        self.IN = inw * (indent or 0)
        self.INp = self.IN + inw
        self.scheme = dict(self.scheme, **scheme)
        self.graph_scheme = dict(self.graph_scheme, root=self.label(self.root))

    def attr(self, name: str, value: Any) -> str:
        value = '"{0}"'.format(value)
        return self.FMT(self._attr, name=name, value=value)

    def attrs(self, d: Dict, scheme: Optional[Dict]=None) -> str:
        d = dict(self.scheme, **dict(scheme, **d or {}) if scheme else d)
        return self._attrsep.join(
            safe_str(self.attr(k, v)) for k, v in d.items()
        )

    def head(self, **attrs: Dict[str, str]) -> str:
        return self.FMT(
            self._head, id=self.id, type=self.type,
            attrs=self.attrs(attrs, self.graph_scheme),
        )

    def tail(self) -> str:
        return self.FMT(self._tail)

    def label(self, obj: Any) -> str:
        return obj

    def node(self, obj: Any, **attrs: Dict[str, str]) -> str:
        return self.draw_node(obj, self.node_scheme, attrs)

    def terminal_node(self, obj: Any, **attrs: Dict[str, str]) -> str:
        return self.draw_node(obj, self.term_scheme, attrs)

    def edge(self, a: Any, b: Any, **attrs: Dict[str, str]) -> str:
        return self.draw_edge(a, b, **attrs)

    def _enc(self, s: str) -> str:
        return s.encode('utf-8', 'ignore').decode()

    def FMT(self, fmt: str, *args, **kwargs) -> str:
        return self._enc(fmt.format(
            *args, **dict(kwargs, IN=self.IN, INp=self.INp)
        ))

    def draw_edge(self, a: Any, b: Any,
                  scheme: Optional[Dict]=None,
                  attrs: Optional[Dict]=None) -> str:
        return self.FMT(
            self._edge, self.label(a), self.label(b),
            dir=self.direction, attrs=self.attrs(attrs, self.edge_scheme),
        )

    def draw_node(self, obj: Any,
                  scheme: Optional[Dict]=None,
                  attrs: Optional[Dict]=None) -> str:
        return self.FMT(
            self._node, self.label(obj), attrs=self.attrs(attrs, scheme),
        )
