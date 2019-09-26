from __future__ import absolute_import, unicode_literals

import sys

import pytest

from case import Mock, skip
from celery.five import PY3, long_t, python_2_unicode_compatible, string
from celery.local import PromiseProxy, Proxy, maybe_evaluate, try_import


class test_try_import:

    def test_imports(self):
        assert try_import(__name__)

    def test_when_default(self):
        default = object()
        assert try_import('foobar.awqewqe.asdwqewq', default) is default


class test_Proxy:

    def test_std_class_attributes(self):
        assert Proxy.__name__ == 'Proxy'
        assert Proxy.__module__ == 'celery.local'
        assert isinstance(Proxy.__doc__, str)

    def test_doc(self):
        def real():
            pass
        x = Proxy(real, __doc__='foo')
        assert x.__doc__ == 'foo'

    def test_name(self):

        def real():
            """real function"""
            return 'REAL'

        x = Proxy(lambda: real, name='xyz')
        assert x.__name__ == 'xyz'

        y = Proxy(lambda: real)
        assert y.__name__ == 'real'

        assert x.__doc__ == 'real function'

        assert x.__class__ == type(real)
        assert x.__dict__ == real.__dict__
        assert repr(x) == repr(real)
        assert x.__module__

    def test_get_current_local(self):
        x = Proxy(lambda: 10)
        object.__setattr__(x, '_Proxy_local', Mock())
        assert x._get_current_object()

    def test_bool(self):

        class X(object):

            def __bool__(self):
                return False
            __nonzero__ = __bool__

        x = Proxy(lambda: X())
        assert not x

    def test_slots(self):

        class X(object):
            __slots__ = ()

        x = Proxy(X)
        with pytest.raises(AttributeError):
            x.__dict__

    @skip.if_python3()
    def test_unicode(self):

        @python_2_unicode_compatible
        class X(object):

            def __unicode__(self):
                return 'UNICODE'
            __str__ = __unicode__

            def __repr__(self):
                return 'REPR'

        x = Proxy(lambda: X())
        assert string(x) == 'UNICODE'
        del(X.__unicode__)
        del(X.__str__)
        assert string(x) == 'REPR'

    def test_dir(self):

        class X(object):

            def __dir__(self):
                return ['a', 'b', 'c']

        x = Proxy(lambda: X())
        assert dir(x) == ['a', 'b', 'c']

        class Y(object):

            def __dir__(self):
                raise RuntimeError()
        y = Proxy(lambda: Y())
        assert dir(y) == []

    def test_getsetdel_attr(self):

        class X(object):
            a = 1
            b = 2
            c = 3

            def __dir__(self):
                return ['a', 'b', 'c']

        v = X()

        x = Proxy(lambda: v)
        assert x.__members__ == ['a', 'b', 'c']
        assert x.a == 1
        assert x.b == 2
        assert x.c == 3

        setattr(x, 'a', 10)
        assert x.a == 10

        del(x.a)
        assert x.a == 1

    def test_dictproxy(self):
        v = {}
        x = Proxy(lambda: v)
        x['foo'] = 42
        assert x['foo'] == 42
        assert len(x) == 1
        assert 'foo' in x
        del(x['foo'])
        with pytest.raises(KeyError):
            x['foo']
        assert iter(x)

    def test_listproxy(self):
        v = []
        x = Proxy(lambda: v)
        x.append(1)
        x.extend([2, 3, 4])
        assert x[0] == 1
        assert x[:-1] == [1, 2, 3]
        del(x[-1])
        assert x[:-1] == [1, 2]
        x[0] = 10
        assert x[0] == 10
        assert 10 in x
        assert len(x) == 3
        assert iter(x)
        x[0:2] = [1, 2]
        del(x[0:2])
        assert str(x)
        if sys.version_info[0] < 3:
            assert x.__cmp__(object()) == -1

    def test_complex_cast(self):

        class O(object):

            def __complex__(self):
                return complex(10.333)

        o = Proxy(O)
        assert o.__complex__() == complex(10.333)

    def test_index(self):

        class O(object):

            def __index__(self):
                return 1

        o = Proxy(O)
        assert o.__index__() == 1

    def test_coerce(self):

        class O(object):

            def __coerce__(self, other):
                return self, other

        o = Proxy(O)
        assert o.__coerce__(3)

    def test_int(self):
        assert Proxy(lambda: 10) + 1 == Proxy(lambda: 11)
        assert Proxy(lambda: 10) - 1 == Proxy(lambda: 9)
        assert Proxy(lambda: 10) * 2 == Proxy(lambda: 20)
        assert Proxy(lambda: 10) ** 2 == Proxy(lambda: 100)
        assert Proxy(lambda: 20) / 2 == Proxy(lambda: 10)
        assert Proxy(lambda: 20) // 2 == Proxy(lambda: 10)
        assert Proxy(lambda: 11) % 2 == Proxy(lambda: 1)
        assert Proxy(lambda: 10) << 2 == Proxy(lambda: 40)
        assert Proxy(lambda: 10) >> 2 == Proxy(lambda: 2)
        assert Proxy(lambda: 10) ^ 7 == Proxy(lambda: 13)
        assert Proxy(lambda: 10) | 40 == Proxy(lambda: 42)
        assert Proxy(lambda: 10) != Proxy(lambda: -11)
        assert Proxy(lambda: 10) != Proxy(lambda: -10)
        assert Proxy(lambda: -10) == Proxy(lambda: -10)

        assert Proxy(lambda: 10) < Proxy(lambda: 20)
        assert Proxy(lambda: 20) > Proxy(lambda: 10)
        assert Proxy(lambda: 10) >= Proxy(lambda: 10)
        assert Proxy(lambda: 10) <= Proxy(lambda: 10)
        assert Proxy(lambda: 10) == Proxy(lambda: 10)
        assert Proxy(lambda: 20) != Proxy(lambda: 10)
        assert Proxy(lambda: 100).__divmod__(30)
        assert Proxy(lambda: 100).__truediv__(30)
        assert abs(Proxy(lambda: -100))

        x = Proxy(lambda: 10)
        x -= 1
        assert x == 9
        x = Proxy(lambda: 9)
        x += 1
        assert x == 10
        x = Proxy(lambda: 10)
        x *= 2
        assert x == 20
        x = Proxy(lambda: 20)
        x /= 2
        assert x == 10
        x = Proxy(lambda: 10)
        x %= 2
        assert x == 0
        x = Proxy(lambda: 10)
        x <<= 3
        assert x == 80
        x = Proxy(lambda: 80)
        x >>= 4
        assert x == 5
        x = Proxy(lambda: 5)
        x ^= 1
        assert x == 4
        x = Proxy(lambda: 4)
        x **= 4
        assert x == 256
        x = Proxy(lambda: 256)
        x //= 2
        assert x == 128
        x = Proxy(lambda: 128)
        x |= 2
        assert x == 130
        x = Proxy(lambda: 130)
        x &= 10
        assert x == 2

        x = Proxy(lambda: 10)
        assert type(x.__float__()) == float
        assert type(x.__int__()) == int
        if not PY3:
            assert type(x.__long__()) == long_t
        assert hex(x)
        assert oct(x)

    def test_hash(self):

        class X(object):

            def __hash__(self):
                return 1234

        assert hash(Proxy(lambda: X())) == 1234

    def test_call(self):

        class X(object):

            def __call__(self):
                return 1234

        assert Proxy(lambda: X())() == 1234

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
            assert val == 1234
        assert x.entered
        assert x.exited

    def test_reduce(self):

        class X(object):

            def __reduce__(self):
                return 123

        x = Proxy(lambda: X())
        assert x.__reduce__() == 123


class test_PromiseProxy:

    def test_only_evaluated_once(self):

        class X(object):
            attr = 123
            evals = 0

            def __init__(self):
                self.__class__.evals += 1

        p = PromiseProxy(X)
        assert p.attr == 123
        assert p.attr == 123
        assert X.evals == 1

    def test_callbacks(self):
        source = Mock(name='source')
        p = PromiseProxy(source)
        cbA = Mock(name='cbA')
        cbB = Mock(name='cbB')
        cbC = Mock(name='cbC')
        p.__then__(cbA, p)
        p.__then__(cbB, p)
        assert not p.__evaluated__()
        assert object.__getattribute__(p, '__pending__')

        assert repr(p)
        assert p.__evaluated__()
        with pytest.raises(AttributeError):
            object.__getattribute__(p, '__pending__')
        cbA.assert_called_with(p)
        cbB.assert_called_with(p)

        assert p.__evaluated__()
        p.__then__(cbC, p)
        cbC.assert_called_with(p)

        with pytest.raises(AttributeError):
            object.__getattribute__(p, '__pending__')

    def test_maybe_evaluate(self):
        x = PromiseProxy(lambda: 30)
        assert not x.__evaluated__()
        assert maybe_evaluate(x) == 30
        assert maybe_evaluate(x) == 30

        assert maybe_evaluate(30) == 30
        assert x.__evaluated__()
