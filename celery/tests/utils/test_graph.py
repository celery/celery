from __future__ import absolute_import, unicode_literals

from celery.five import WhateverIO, items
from celery.utils.graph import DependencyGraph

from celery.tests.case import Case, Mock


class test_DependencyGraph(Case):

    def graph1(self):
        return DependencyGraph([
            ('A', []),
            ('B', []),
            ('C', ['A']),
            ('D', ['C', 'B']),
        ])

    def test_repr(self):
        self.assertTrue(repr(self.graph1()))

    def test_topsort(self):
        order = self.graph1().topsort()
        # C must start before D
        self.assertLess(order.index('C'), order.index('D'))
        # and B must start before D
        self.assertLess(order.index('B'), order.index('D'))
        # and A must start before C
        self.assertLess(order.index('A'), order.index('C'))

    def test_edges(self):
        self.assertItemsEqual(
            list(self.graph1().edges()),
            ['C', 'D'],
        )

    def test_connect(self):
        x, y = self.graph1(), self.graph1()
        x.connect(y)

    def test_valency_of_when_missing(self):
        x = self.graph1()
        self.assertEqual(x.valency_of('foobarbaz'), 0)

    def test_format(self):
        x = self.graph1()
        x.formatter = Mock()
        obj = Mock()
        self.assertTrue(x.format(obj))
        x.formatter.assert_called_with(obj)
        x.formatter = None
        self.assertIs(x.format(obj), obj)

    def test_items(self):
        self.assertDictEqual(
            dict(items(self.graph1())),
            {'A': [], 'B': [], 'C': ['A'], 'D': ['C', 'B']},
        )

    def test_repr_node(self):
        x = self.graph1()
        self.assertTrue(x.repr_node('fasdswewqewq'))

    def test_to_dot(self):
        s = WhateverIO()
        self.graph1().to_dot(s)
        self.assertTrue(s.getvalue())
