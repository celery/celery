from __future__ import absolute_import

import pickle
import sys

from itertools import count

from kombu.utils.functional import lazy

from celery.five import THREAD_TIMEOUT_MAX, items, range, nextfun
from celery.utils.functional import (
    DummyContext,
    LRUCache,
    head_from_fun,
    firstmethod,
    first,
    maybe_list,
    memoize,
    mlazy,
    padlist,
    regen,
)

from celery.tests.case import Case, SkipTest


class test_DummyContext(Case):

    def test_context(self):
        with DummyContext():
            pass
        with self.assertRaises(KeyError):
            with DummyContext():
                raise KeyError()


class test_LRUCache(Case):

    def test_expires(self):
        limit = 100
        x = LRUCache(limit=limit)
        slots = list(range(limit * 2))
        for i in slots:
            x[i] = i
        self.assertListEqual(list(x.keys()), list(slots[limit:]))
        self.assertTrue(x.items())
        self.assertTrue(x.values())

    def test_is_pickleable(self):
        x = LRUCache(limit=10)
        x.update(luke=1, leia=2)
        y = pickle.loads(pickle.dumps(x))
        self.assertEqual(y.limit, y.limit)
        self.assertEqual(y, x)

    def test_update_expires(self):
        limit = 100
        x = LRUCache(limit=limit)
        slots = list(range(limit * 2))
        for i in slots:
            x.update({i: i})

        self.assertListEqual(list(x.keys()), list(slots[limit:]))

    def test_least_recently_used(self):
        x = LRUCache(3)

        x[1], x[2], x[3] = 1, 2, 3
        self.assertEqual(list(x.keys()), [1, 2, 3])

        x[4], x[5] = 4, 5
        self.assertEqual(list(x.keys()), [3, 4, 5])

        # access 3, which makes it the last used key.
        x[3]
        x[6] = 6
        self.assertEqual(list(x.keys()), [5, 3, 6])

        x[7] = 7
        self.assertEqual(list(x.keys()), [3, 6, 7])

    def test_update_larger_than_cache_size(self):
        x = LRUCache(2)
        x.update({x: x for x in range(100)})
        self.assertEqual(list(x.keys()), [98, 99])

    def assertSafeIter(self, method, interval=0.01, size=10000):
        if sys.version_info >= (3, 5):
            raise SkipTest('Fails on Py3.5')
        from threading import Thread, Event
        from time import sleep
        x = LRUCache(size)
        x.update(zip(range(size), range(size)))

        class Burglar(Thread):

            def __init__(self, cache):
                self.cache = cache
                self.__is_shutdown = Event()
                self.__is_stopped = Event()
                Thread.__init__(self)

            def run(self):
                while not self.__is_shutdown.isSet():
                    try:
                        self.cache.popitem(last=False)
                    except KeyError:
                        break
                self.__is_stopped.set()

            def stop(self):
                self.__is_shutdown.set()
                self.__is_stopped.wait()
                self.join(THREAD_TIMEOUT_MAX)

        burglar = Burglar(x)
        burglar.start()
        try:
            for _ in getattr(x, method)():
                sleep(0.0001)
        finally:
            burglar.stop()

    def test_safe_to_remove_while_iteritems(self):
        self.assertSafeIter('iteritems')

    def test_safe_to_remove_while_keys(self):
        self.assertSafeIter('keys')

    def test_safe_to_remove_while_itervalues(self):
        self.assertSafeIter('itervalues')

    def test_items(self):
        c = LRUCache()
        c.update(a=1, b=2, c=3)
        self.assertTrue(list(items(c)))


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


class test_memoize(Case):

    def test_memoize(self):
        counter = count(1)

        @memoize(maxsize=2)
        def x(i):
            return next(counter)

        self.assertEqual(x(1), 1)
        self.assertEqual(x(1), 1)
        self.assertEqual(x(2), 2)
        self.assertEqual(x(3), 3)
        self.assertEqual(x(1), 4)
        x.clear()
        self.assertEqual(x(3), 5)


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
