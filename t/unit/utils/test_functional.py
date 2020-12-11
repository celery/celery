import types

import pytest
from kombu.utils.functional import lazy

from celery.utils.functional import (DummyContext, first, firstmethod,
                                     fun_accepts_kwargs, fun_takes_argument,
                                     head_from_fun, maybe_list, mlazy,
                                     padlist, regen, seq_concat_item,
                                     seq_concat_seq)


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

        class A:

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
    p = mlazy(it.__next__)
    assert p() == 20
    assert p.evaluated
    assert p() == 20
    assert repr(p) == '20'


class test_regen:

    def setup_method(self):
        self.N = 10
        self.g = regen(i for i in range(self.N))

    def test_regen_list(self):
        l = [1, 2]
        r = regen(iter(l))
        assert regen(l) is l
        # Check equality multiple times to ensure we have concretised properly
        assert r == l
        assert r == l
        assert r.__length_hint__() == 0

        fun, args = r.__reduce__()
        assert fun(*args) == l

    def test_regen_gen_index(self):
        # Index backward from an index smaller than the total size of the
        # concretised generator so we do a partial concretisation
        for i in range(self.N // 2, 0 - 1, -1):
            assert self.g[i] == i
        # Confirm that the generator is not fully consumed
        assert not self.g.fully_consumed()
        # Access `.data` to concretise the remainder of the generator
        assert self.g.data == list(range(self.N))
        assert self.g.fully_consumed()
        # And then confirm indexing and iteration continue to return the fully
        # conretised set of elements
        for i in range(self.N):
            assert self.g[i] == i
        assert list(self.g) == list(range(self.N))

    def test_regen_gen_index_error(self):
        with pytest.raises(IndexError):
            self.g[self.N]  # `N` is out of range
        # Confirm that we concretised the whole generator
        assert self.g.fully_consumed()
        assert list(self.g) == list(range(self.N))

    def test_regen_gen_negative_index(self):
        assert self.g[-1] == self.N - 1
        assert self.g.fully_consumed()
        for i in range(self.N - 1, 0 - 1, -1):
            assert self.g[i] == i

    def test_regen_gen_iter(self):
        list(iter(self.g))
        # Confirm that we concretised the whole generator
        assert self.g.fully_consumed()
        assert list(self.g) == list(range(self.N))

    def test_regen_gen_repr(self):
        # Confirm that `__repr__()` does not consume any elements
        g = regen(i for i in range(1))
        repr(self.g)
        assert not self.g.fully_consumed()

    def test_regen_gen_nonzero(self):
        # Confirm that `__nonzero__()` does not consume any elements
        g = regen(i for i in range(1))
        bool(self.g)
        assert not self.g.fully_consumed()


class test_head_from_fun:

    def test_from_cls(self):
        class X:
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

    def test_regression_3678(self):
        local = {}
        fun = ('def f(foo, *args, bar="", **kwargs):'
               '    return foo, args, bar')
        exec(fun, {}, local)

        g = head_from_fun(local['f'])
        g(1)
        g(1, 2, 3, 4, bar=100)
        with pytest.raises(TypeError):
            g(bar=100)

    def test_from_fun_with_hints(self):
        local = {}
        fun = ('def f_hints(x: int, y: int, kwarg: int=1):'
               '    pass')
        exec(fun, {}, local)
        f_hints = local['f_hints']

        g = head_from_fun(f_hints)
        with pytest.raises(TypeError):
            g(1)
        g(1, 2)
        g(1, 2, kwarg=3)

    def test_from_fun_forced_kwargs(self):
        local = {}
        fun = ('def f_kwargs(*, a, b="b", c=None):'
               '    return')
        exec(fun, {}, local)
        f_kwargs = local['f_kwargs']

        g = head_from_fun(f_kwargs)
        with pytest.raises(TypeError):
            g(1)

        g(a=1)
        g(a=1, b=2)
        g(a=1, b=2, c=3)

    def test_classmethod(self):
        class A:
            @classmethod
            def f(cls, x):
                return x

        fun = head_from_fun(A.f, bound=False)
        assert fun(A, 1) == 1

        fun = head_from_fun(A.f, bound=True)
        assert fun(1) == 1


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


@pytest.mark.parametrize('a,b,expected', [
    ((1, 2, 3), [4, 5], (1, 2, 3, 4, 5)),
    ((1, 2), [3, 4, 5], [1, 2, 3, 4, 5]),
    ([1, 2, 3], (4, 5), [1, 2, 3, 4, 5]),
    ([1, 2], (3, 4, 5), (1, 2, 3, 4, 5)),
])
def test_seq_concat_seq(a, b, expected):
    res = seq_concat_seq(a, b)
    assert type(res) is type(expected)  # noqa
    assert res == expected


@pytest.mark.parametrize('a,b,expected', [
    ((1, 2, 3), 4, (1, 2, 3, 4)),
    ([1, 2, 3], 4, [1, 2, 3, 4]),
])
def test_seq_concat_item(a, b, expected):
    res = seq_concat_item(a, b)
    assert type(res) is type(expected)  # noqa
    assert res == expected


class StarKwargsCallable:

    def __call__(self, **kwargs):
        return 1


class StarArgsStarKwargsCallable:

    def __call__(self, *args, **kwargs):
        return 1


class StarArgsCallable:

    def __call__(self, *args):
        return 1


class ArgsCallable:

    def __call__(self, a, b):
        return 1


class ArgsStarKwargsCallable:

    def __call__(self, a, b, **kwargs):
        return 1


class test_fun_accepts_kwargs:

    @pytest.mark.parametrize('fun', [
        lambda a, b, **kwargs: 1,
        lambda *args, **kwargs: 1,
        lambda foo=1, **kwargs: 1,
        StarKwargsCallable(),
        StarArgsStarKwargsCallable(),
        ArgsStarKwargsCallable(),
    ])
    def test_accepts(self, fun):
        assert fun_accepts_kwargs(fun)

    @pytest.mark.parametrize('fun', [
        lambda a: 1,
        lambda a, b: 1,
        lambda *args: 1,
        lambda a, kw1=1, kw2=2: 1,
        StarArgsCallable(),
        ArgsCallable(),
    ])
    def test_rejects(self, fun):
        assert not fun_accepts_kwargs(fun)
