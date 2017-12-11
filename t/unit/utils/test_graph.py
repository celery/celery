<<<<<<< HEAD
from __future__ import absolute_import, unicode_literals

from case import Mock

from celery.five import WhateverIO, items
=======
import io
from case import Mock
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from celery.utils.graph import DependencyGraph


class test_DependencyGraph:

    def graph1(self):
        return DependencyGraph([
            ('A', []),
            ('B', []),
            ('C', ['A']),
            ('D', ['C', 'B']),
        ])

    def test_repr(self):
        assert repr(self.graph1())

    def test_topsort(self):
        order = self.graph1().topsort()
        # C must start before D
        assert order.index('C') < order.index('D')
        # and B must start before D
        assert order.index('B') < order.index('D')
        # and A must start before C
        assert order.index('A') < order.index('C')

    def test_edges(self):
        assert sorted(list(self.graph1().edges())) == ['C', 'D']

    def test_connect(self):
        x, y = self.graph1(), self.graph1()
        x.connect(y)

    def test_valency_of_when_missing(self):
        x = self.graph1()
        assert x.valency_of('foobarbaz') == 0

    def test_format(self):
        x = self.graph1()
        x.formatter = Mock()
        obj = Mock()
        assert x.format(obj)
        x.formatter.assert_called_with(obj)
        x.formatter = None
        assert x.format(obj) is obj

    def test_items(self):
        assert dict(self.graph1().items()) == {
            'A': [], 'B': [], 'C': ['A'], 'D': ['C', 'B'],
        }

    def test_repr_node(self):
        x = self.graph1()
        assert x.repr_node('fasdswewqewq')

    def test_to_dot(self):
        s = io.StringIO()
        self.graph1().to_dot(s)
        assert s.getvalue()
