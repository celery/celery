from __future__ import absolute_import, unicode_literals

from case import Mock
from celery.five import WhateverIO, items
from celery.utils.graph import DependencyGraph


class test_DependencyGraph:

    def graph1(self):
        res_a = self.app.AsyncResult('A')
        res_b = self.app.AsyncResult('B')
        res_c = self.app.GroupResult('C', [res_a])
        res_d = self.app.GroupResult('D', [res_c, res_b])
        node_a = (res_a, [])
        node_b = (res_b, [])
        node_c = (res_c, [res_a])
        node_d = (res_d, [res_c, res_b])
        return DependencyGraph([
            node_a,
            node_b,
            node_c,
            node_d,
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
        edges = self.graph1().edges()
        assert sorted(edges, key=str) == ['C', 'D']

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
        assert dict(items(self.graph1())) == {
            'A': [], 'B': [], 'C': ['A'], 'D': ['C', 'B'],
        }

    def test_repr_node(self):
        x = self.graph1()
        assert x.repr_node('fasdswewqewq')

    def test_to_dot(self):
        s = WhateverIO()
        self.graph1().to_dot(s)
        assert s.getvalue()
