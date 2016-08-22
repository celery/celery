from __future__ import absolute_import, unicode_literals

from celery.five import range
from celery.utils.dispatch.saferef import safe_ref


class Class1(object):

    def x(self):
        pass


def fun(obj):
    pass


class Class2(object):

    def __call__(self, obj):
        pass


class test_safe_ref:

    def setup(self):
        ts = []
        ss = []
        for x in range(5000):
            t = Class1()
            ts.append(t)
            s = safe_ref(t.x, self._closure)
            ss.append(s)
        ts.append(fun)
        ss.append(safe_ref(fun, self._closure))
        for x in range(30):
            t = Class2()
            ts.append(t)
            s = safe_ref(t, self._closure)
            ss.append(s)
        self.ts = ts
        self.ss = ss
        self.closureCount = 0

    def test_in(self):
        """test_in

        Test the "in" operator for safe references (cmp)

        """
        for t in self.ts[:50]:
            assert safe_ref(t.x) in self.ss

    def test_valid(self):
        """test_value

        Test that the references are valid (return instance methods)

        """
        for s in self.ss:
            assert s()

    def test_shortcircuit(self):
        """test_shortcircuit

        Test that creation short-circuits to reuse existing references

        """
        sd = {}
        for s in self.ss:
            sd[s] = 1
        for t in self.ts:
            if hasattr(t, 'x'):
                assert safe_ref(t.x) in sd
            else:
                assert safe_ref(t) in sd

    def test_representation(self):
        """test_representation

        Test that the reference object's representation works

        XXX Doesn't currently check the results, just that no error
            is raised
        """
        repr(self.ss[-1])

    def _closure(self, ref):
        """Dumb utility mechanism to increment deletion counter"""
        self.closureCount += 1
