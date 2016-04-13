from __future__ import absolute_import, unicode_literals

from kombu.utils.functional import lazy

from celery.five import range, nextfun
from celery.utils.functional import (
    DummyContext,
    fun_takes_argument,
    head_from_fun,
    firstmethod,
    first,
    maybe_list,
    mlazy,
    padlist,
    regen,
)

from celery.tests.case import Case


class test_DummyContext(Case):

    def test_context(self):
        with DummyContext():
            pass
        with self.assertRaises(KeyError):
            with DummyContext():
                raise KeyError()


class test_utils(Case):

    def test_padlist(self):
        self.assertListEqual(
            padlist(['George', 'Costanza', 'NYC'], 3),
            ['George', 'Costanza', 'NYC'],
        )
        self.assertListEqual(
            padlist(['George', 'Costanza'], 3),
            ['George', 'Costanza', None],
        )
        self.assertListEqual(
            padlist(['George', 'Costanza', 'NYC'], 4, default='Earth'),
            ['George', 'Costanza', 'NYC', 'Earth'],
        )

    def test_firstmethod_AttributeError(self):
        self.assertIsNone(firstmethod('foo')([object()]))

    def test_firstmethod_handles_lazy(self):

        class A(object):

            def __init__(self, value=None):
                self.value = value

            def m(self):
                return self.value

        self.assertEqual('four', firstmethod('m')([
            A(), A(), A(), A('four'), A('five')]))
        self.assertEqual('four', firstmethod('m')([
            A(), A(), A(), lazy(lambda: A('four')), A('five')]))

    def test_first(self):
        iterations = [0]

        def predicate(value):
            iterations[0] += 1
            if value == 5:
                return True
            return False

        self.assertEqual(5, first(predicate, range(10)))
        self.assertEqual(iterations[0], 6)

        iterations[0] = 0
        self.assertIsNone(first(predicate, range(10, 20)))
        self.assertEqual(iterations[0], 10)

    def test_maybe_list(self):
        self.assertEqual(maybe_list(1), [1])
        self.assertEqual(maybe_list([1]), [1])
        self.assertIsNone(maybe_list(None))


class test_mlazy(Case):

    def test_is_memoized(self):

        it = iter(range(20, 30))
        p = mlazy(nextfun(it))
        self.assertEqual(p(), 20)
        self.assertTrue(p.evaluated)
        self.assertEqual(p(), 20)
        self.assertEqual(repr(p), '20')


class test_regen(Case):

    def test_regen_list(self):
        l = [1, 2]
        r = regen(iter(l))
        self.assertIs(regen(l), l)
        self.assertEqual(r, l)
        self.assertEqual(r, l)
        self.assertEqual(r.__length_hint__(), 0)

        fun, args = r.__reduce__()
        self.assertEqual(fun(*args), l)

    def test_regen_gen(self):
        g = regen(iter(list(range(10))))
        self.assertEqual(g[7], 7)
        self.assertEqual(g[6], 6)
        self.assertEqual(g[5], 5)
        self.assertEqual(g[4], 4)
        self.assertEqual(g[3], 3)
        self.assertEqual(g[2], 2)
        self.assertEqual(g[1], 1)
        self.assertEqual(g[0], 0)
        self.assertEqual(g.data, list(range(10)))
        self.assertEqual(g[8], 8)
        self.assertEqual(g[0], 0)
        g = regen(iter(list(range(10))))
        self.assertEqual(g[0], 0)
        self.assertEqual(g[1], 1)
        self.assertEqual(g.data, list(range(10)))
        g = regen(iter([1]))
        self.assertEqual(g[0], 1)
        with self.assertRaises(IndexError):
            g[1]
        self.assertEqual(g.data, [1])

        g = regen(iter(list(range(10))))
        self.assertEqual(g[-1], 9)
        self.assertEqual(g[-2], 8)
        self.assertEqual(g[-3], 7)
        self.assertEqual(g[-4], 6)
        self.assertEqual(g[-5], 5)
        self.assertEqual(g[5], 5)
        self.assertEqual(g.data, list(range(10)))

        self.assertListEqual(list(iter(g)), list(range(10)))


class test_head_from_fun(Case):

    def test_from_cls(self):
        class X(object):
            def __call__(x, y, kwarg=1):
                pass

        g = head_from_fun(X())
        with self.assertRaises(TypeError):
            g(1)
        g(1, 2)
        g(1, 2, kwarg=3)

    def test_from_fun(self):
        def f(x, y, kwarg=1):
            pass
        g = head_from_fun(f)
        with self.assertRaises(TypeError):
            g(1)
        g(1, 2)
        g(1, 2, kwarg=3)

    def test_from_fun_with_hints(self):
        local = {}
        fun = ('def f_hints(x: int, y: int, kwarg: int=1):'
               '    pass')
        try:
            exec(fun, {}, local)
        except SyntaxError:
            # py2
            return
        f_hints = local['f_hints']

        g = head_from_fun(f_hints)
        with self.assertRaises(TypeError):
            g(1)
        g(1, 2)
        g(1, 2, kwarg=3)


class test_fun_takes_argument(Case):

    def test_starkwargs(self):
        self.assertTrue(fun_takes_argument('foo', lambda **kw: 1))

    def test_named(self):
        self.assertTrue(fun_takes_argument('foo', lambda a, foo, bar: 1))

        def fun(a, b, c, d):
            return 1

        self.assertTrue(fun_takes_argument('foo', fun, position=4))

    def test_starargs(self):
        self.assertTrue(fun_takes_argument('foo', lambda a, *args: 1))

    def test_does_not(self):
        self.assertFalse(fun_takes_argument('foo', lambda a, bar, baz: 1))
        self.assertFalse(fun_takes_argument('foo', lambda: 1))

        def fun(a, b, foo):
            return 1

        self.assertFalse(fun_takes_argument('foo', fun, position=4))
