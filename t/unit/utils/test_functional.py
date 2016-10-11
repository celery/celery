from __future__ import absolute_import, unicode_literals

import pytest

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


def test_DummyContext():
    with DummyContext():
        pass
    with pytest.raises(KeyError):
        with DummyContext():
            raise KeyError()


@pytest.mark.parametrize('items,n,default,expected', [
    (['George', 'Costanza', 'NYC'], 3, None,
     ['George', 'Costanza', 'NYC']),
    (['George', 'Costanza'], 3, None,
     ['George', 'Costanza', None]),
    (['George', 'Costanza', 'NYC'], 4, 'Earth',
     ['George', 'Costanza', 'NYC', 'Earth']),
])
def test_padlist(items, n, default, expected):
    assert padlist(items, n, default=default) == expected


class test_firstmethod:

    def test_AttributeError(self):
        assert firstmethod('foo')([object()]) is None

    def test_handles_lazy(self):

        class A(object):

            def __init__(self, value=None):
                self.value = value

            def m(self):
                return self.value

        assert 'four' == firstmethod('m')([
            A(), A(), A(), A('four'), A('five')])
        assert 'four' == firstmethod('m')([
            A(), A(), A(), lazy(lambda: A('four')), A('five')])


def test_first():
    iterations = [0]

    def predicate(value):
        iterations[0] += 1
        if value == 5:
            return True
        return False

    assert first(predicate, range(10)) == 5
    assert iterations[0] == 6

    iterations[0] = 0
    assert first(predicate, range(10, 20)) is None
    assert iterations[0] == 10


def test_maybe_list():
    assert maybe_list(1) == [1]
    assert maybe_list([1]) == [1]
    assert maybe_list(None) is None


def test_mlazy():
    it = iter(range(20, 30))
    p = mlazy(nextfun(it))
    assert p() == 20
    assert p.evaluated
    assert p() == 20
    assert repr(p) == '20'


class test_regen:

    def test_list(self):
        l = [1, 2]
        r = regen(iter(l))
        assert regen(l) is l
        assert r == l
        assert r == l  # again
        assert r.__length_hint__() == 0

        fun, args = r.__reduce__()
        assert fun(*args) == l

    def test_gen(self):
        g = regen(iter(list(range(10))))
        assert g[7] == 7
        assert g[6] == 6
        assert g[5] == 5
        assert g[4] == 4
        assert g[3] == 3
        assert g[2] == 2
        assert g[1] == 1
        assert g[0] == 0
        assert g.data, list(range(10))
        assert g[8] == 8
        assert g[0] == 0
        g = regen(iter(list(range(10))))
        assert g[0] == 0
        assert g[1] == 1
        assert g.data == list(range(10))
        g = regen(iter([1]))
        assert g[0] == 1
        with pytest.raises(IndexError):
            g[1]
        assert g.data == [1]

        g = regen(iter(list(range(10))))
        assert g[-1] == 9
        assert g[-2] == 8
        assert g[-3] == 7
        assert g[-4] == 6
        assert g[-5] == 5
        assert g[5] == 5
        assert g.data == list(range(10))

        assert list(iter(g)) == list(range(10))


class test_head_from_fun:

    def test_from_cls(self):
        class X(object):
            def __call__(x, y, kwarg=1):  # noqa
                pass

        g = head_from_fun(X())
        with pytest.raises(TypeError):
            g(1)
        g(1, 2)
        g(1, 2, kwarg=3)

    def test_from_fun(self):
        def f(x, y, kwarg=1):
            pass
        g = head_from_fun(f)
        with pytest.raises(TypeError):
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
        with pytest.raises(TypeError):
            g(1)
        g(1, 2)
        g(1, 2, kwarg=3)


class test_fun_takes_argument:

    def test_starkwargs(self):
        assert fun_takes_argument('foo', lambda **kw: 1)

    def test_named(self):
        assert fun_takes_argument('foo', lambda a, foo, bar: 1)

        def fun(a, b, c, d):
            return 1

        assert fun_takes_argument('foo', fun, position=4)

    def test_starargs(self):
        assert fun_takes_argument('foo', lambda a, *args: 1)

    def test_does_not(self):
        assert not fun_takes_argument('foo', lambda a, bar, baz: 1)
        assert not fun_takes_argument('foo', lambda: 1)

        def fun(a, b, foo):
            return 1

        assert not fun_takes_argument('foo', fun, position=4)
