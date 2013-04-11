from __future__ import absolute_import
from __future__ import with_statement

import sys

from nose import SkipTest

from celery.local import Proxy, PromiseProxy, maybe_evaluate, try_import

from celery.tests.utils import Case


class test_try_import(Case):

    def test_imports(self):
        self.assertTrue(try_import(__name__))

    def test_when_default(self):
        default = object()
        self.assertIs(try_import('foobar.awqewqe.asdwqewq', default), default)


class test_Proxy(Case):

    def test_std_class_attributes(self):
        self.assertEqual(Proxy.__name__, 'Proxy')
        self.assertEqual(Proxy.__module__, 'celery.local')
        self.assertIsInstance(Proxy.__doc__, str)

    def test_name(self):

        def real():
            """real function"""
            return 'REAL'

        x = Proxy(lambda: real, name='xyz')
        self.assertEqual(x.__name__, 'xyz')

        y = Proxy(lambda: real)
        self.assertEqual(y.__name__, 'real')

        self.assertEqual(x.__doc__, 'real function')

        self.assertEqual(x.__class__, type(real))
        self.assertEqual(x.__dict__, real.__dict__)
        self.assertEqual(repr(x), repr(real))

    def test_nonzero(self):

        class X(object):

            def __nonzero__(self):
                return False

        x = Proxy(lambda: X())
        self.assertFalse(x)

    def test_slots(self):

        class X(object):
            __slots__ = ()

        x = Proxy(X)
        with self.assertRaises(AttributeError):
            x.__dict__

    def test_unicode(self):

        class X(object):

            def __unicode__(self):
                return u'UNICODE'

            def __repr__(self):
                return 'REPR'

        x = Proxy(lambda: X())
        self.assertEqual(unicode(x), u'UNICODE')
        del(X.__unicode__)
        self.assertEqual(unicode(x), 'REPR')

    def test_dir(self):
        if sys.version_info < (2, 6):
            raise SkipTest('Not relevant for Py2.5')

        class X(object):

            def __dir__(self):
                return ['a', 'b', 'c']

        x = Proxy(lambda: X())
        self.assertListEqual(dir(x), ['a', 'b', 'c'])

        class Y(object):

            def __dir__(self):
                raise RuntimeError()
        y = Proxy(lambda: Y())
        self.assertListEqual(dir(y), [])

    def test_getsetdel_attr(self):
        if sys.version_info < (2, 6):
            raise SkipTest('Not relevant for Py2.5')

        class X(object):
            a = 1
            b = 2
            c = 3

            def __dir__(self):
                return ['a', 'b', 'c']

        v = X()

        x = Proxy(lambda: v)
        self.assertListEqual(x.__members__, ['a', 'b', 'c'])
        self.assertEqual(x.a, 1)
        self.assertEqual(x.b, 2)
        self.assertEqual(x.c, 3)

        setattr(x, 'a', 10)
        self.assertEqual(x.a, 10)

        del(x.a)
        self.assertEqual(x.a, 1)

    def test_dictproxy(self):
        v = {}
        x = Proxy(lambda: v)
        x['foo'] = 42
        self.assertEqual(x['foo'], 42)
        self.assertEqual(len(x), 1)
        self.assertIn('foo', x)
        del(x['foo'])
        with self.assertRaises(KeyError):
            x['foo']
        self.assertTrue(iter(x))

    def test_listproxy(self):
        v = []
        x = Proxy(lambda: v)
        x.append(1)
        x.extend([2, 3, 4])
        self.assertEqual(x[0], 1)
        self.assertEqual(x[:-1], [1, 2, 3])
        del(x[-1])
        self.assertEqual(x[:-1], [1, 2])
        x[0] = 10
        self.assertEqual(x[0], 10)
        self.assertIn(10, x)
        self.assertEqual(len(x), 3)
        self.assertTrue(iter(x))

    def test_int(self):
        self.assertEqual(Proxy(lambda: 10) + 1, Proxy(lambda: 11))
        self.assertEqual(Proxy(lambda: 10) - 1, Proxy(lambda: 9))
        self.assertEqual(Proxy(lambda: 10) * 2, Proxy(lambda: 20))
        self.assertEqual(Proxy(lambda: 10) ** 2, Proxy(lambda: 100))
        self.assertEqual(Proxy(lambda: 20) / 2, Proxy(lambda: 10))
        self.assertEqual(Proxy(lambda: 20) // 2, Proxy(lambda: 10))
        self.assertEqual(Proxy(lambda: 11) % 2, Proxy(lambda: 1))
        self.assertEqual(Proxy(lambda: 10) << 2, Proxy(lambda: 40))
        self.assertEqual(Proxy(lambda: 10) >> 2, Proxy(lambda: 2))
        self.assertEqual(Proxy(lambda: 10) ^ 7, Proxy(lambda: 13))
        self.assertEqual(Proxy(lambda: 10) | 40, Proxy(lambda: 42))
        self.assertEqual(~Proxy(lambda: 10), Proxy(lambda: -11))
        self.assertEqual(-Proxy(lambda: 10), Proxy(lambda: -10))
        self.assertEqual(+Proxy(lambda: -10), Proxy(lambda: -10))
        self.assertTrue(Proxy(lambda: 10) < Proxy(lambda: 20))
        self.assertTrue(Proxy(lambda: 20) > Proxy(lambda: 10))
        self.assertTrue(Proxy(lambda: 10) >= Proxy(lambda: 10))
        self.assertTrue(Proxy(lambda: 10) <= Proxy(lambda: 10))
        self.assertTrue(Proxy(lambda: 10) == Proxy(lambda: 10))
        self.assertTrue(Proxy(lambda: 20) != Proxy(lambda: 10))

        x = Proxy(lambda: 10)
        x -= 1
        self.assertEqual(x, 9)
        x = Proxy(lambda: 9)
        x += 1
        self.assertEqual(x, 10)
        x = Proxy(lambda: 10)
        x *= 2
        self.assertEqual(x, 20)
        x = Proxy(lambda: 20)
        x /= 2
        self.assertEqual(x, 10)
        x = Proxy(lambda: 10)
        x %= 2
        self.assertEqual(x, 0)
        x = Proxy(lambda: 10)
        x <<= 3
        self.assertEqual(x, 80)
        x = Proxy(lambda: 80)
        x >>= 4
        self.assertEqual(x, 5)
        x = Proxy(lambda: 5)
        x ^= 1
        self.assertEqual(x, 4)
        x = Proxy(lambda: 4)
        x **= 4
        self.assertEqual(x, 256)
        x = Proxy(lambda: 256)
        x //= 2
        self.assertEqual(x, 128)
        x = Proxy(lambda: 128)
        x |= 2
        self.assertEqual(x, 130)
        x = Proxy(lambda: 130)
        x &= 10
        self.assertEqual(x, 2)

        x = Proxy(lambda: 10)
        self.assertEqual(type(x.__float__()), float)
        self.assertEqual(type(x.__int__()), int)
        self.assertEqual(type(x.__long__()), long)
        self.assertTrue(hex(x))
        self.assertTrue(oct(x))

    def test_hash(self):

        class X(object):

            def __hash__(self):
                return 1234

        self.assertEqual(hash(Proxy(lambda: X())), 1234)

    def test_call(self):

        class X(object):

            def __call__(self):
                return 1234

        self.assertEqual(Proxy(lambda: X())(), 1234)

    def test_context(self):

        class X(object):
            entered = exited = False

            def __enter__(self):
                self.entered = True
                return 1234

            def __exit__(self, *exc_info):
                self.exited = True

        v = X()
        x = Proxy(lambda: v)
        with x as val:
            self.assertEqual(val, 1234)
        self.assertTrue(x.entered)
        self.assertTrue(x.exited)

    def test_reduce(self):

        class X(object):

            def __reduce__(self):
                return 123

        x = Proxy(lambda: X())
        self.assertEqual(x.__reduce__(), 123)


class test_PromiseProxy(Case):

    def test_only_evaluated_once(self):

        class X(object):
            attr = 123
            evals = 0

            def __init__(self):
                self.__class__.evals += 1

        p = PromiseProxy(X)
        self.assertEqual(p.attr, 123)
        self.assertEqual(p.attr, 123)
        self.assertEqual(X.evals, 1)

    def test_maybe_evaluate(self):
        x = PromiseProxy(lambda: 30)
        self.assertEqual(maybe_evaluate(x), 30)
        self.assertEqual(maybe_evaluate(x), 30)

        self.assertEqual(maybe_evaluate(30), 30)
