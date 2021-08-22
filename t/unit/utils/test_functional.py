import collections

import pytest
import pytest_subtests  # noqa: F401
from kombu.utils.functional import lazy

from celery.utils.functional import (DummyContext, first, firstmethod,
                                     fun_accepts_kwargs, fun_takes_argument,
                                     head_from_fun, lookahead, maybe_list,
                                     mlazy, padlist, regen, seq_concat_item,
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


def test_lookahead():
    assert list(lookahead(x for x in range(6))) == [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, None)]


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

    def test_list(self):
        l = [1, 2]
        r = regen(iter(l))
        assert regen(l) is l
        assert r == l
        assert r == l  # again
        assert r.__length_hint__() == 0

        fun, args = r.__reduce__()
        assert fun(*args) == l

    @pytest.fixture
    def g(self):
        return regen(iter(list(range(10))))

    def test_gen(self, g):
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

    def test_gen__index_2(self, g):
        assert g[0] == 0
        assert g[1] == 1
        assert g.data == list(range(10))

    def test_gen__index_error(self, g):
        assert g[0] == 0
        with pytest.raises(IndexError):
            g[11]
        assert list(iter(g)) == list(range(10))

    def test_gen__negative_index(self, g):
        assert g[-1] == 9
        assert g[-2] == 8
        assert g[-3] == 7
        assert g[-4] == 6
        assert g[-5] == 5
        assert g[5] == 5
        assert g.data == list(range(10))

        assert list(iter(g)) == list(range(10))

    def test_nonzero__does_not_consume_more_than_first_item(self):
        def build_generator():
            yield 1
            pytest.fail("generator should not consume past first item")
            yield 2

        g = regen(build_generator())
        assert bool(g)
        assert g[0] == 1

    def test_nonzero__empty_iter(self):
        assert not regen(iter([]))

    def test_deque(self):
        original_list = [42]
        d = collections.deque(original_list)
        # Confirm that concretising a `regen()` instance repeatedly for an
        # equality check always returns the original list
        g = regen(d)
        assert g == original_list
        assert g == original_list

    def test_repr(self):
        def die():
            raise AssertionError("Generator died")
            yield None

        # Confirm that `regen()` instances are not concretised when represented
        g = regen(die())
        assert "..." in repr(g)

    def test_partial_reconcretisation(self):
        class WeirdIterator():
            def __init__(self, iter_):
                self.iter_ = iter_
                self._errored = False

            def __iter__(self):
                yield from self.iter_
                if not self._errored:
                    try:
                        # This should stop the regen instance from marking
                        # itself as being done
                        raise AssertionError("Iterator errored")
                    finally:
                        self._errored = True

        original_list = list(range(42))
        g = regen(WeirdIterator(original_list))
        iter_g = iter(g)
        for e in original_list:
            assert e == next(iter_g)
        with pytest.raises(AssertionError, match="Iterator errored"):
            next(iter_g)
        # The following checks are for the known "misbehaviour"
        assert getattr(g, "_regen__done") is False
        # If the `regen()` instance doesn't think it's done then it'll dupe the
        # elements from the underlying iterator if it can be re-used
        iter_g = iter(g)
        for e in original_list * 2:
            assert next(iter_g) == e
        with pytest.raises(StopIteration):
            next(iter_g)
        assert getattr(g, "_regen__done") is True
        # Finally we xfail this test to keep track of it
        raise pytest.xfail(reason="#6794")

    def test_length_hint_passthrough(self, g):
        assert g.__length_hint__() == 10

    def test_getitem_repeated(self, g):
        halfway_idx = g.__length_hint__() // 2
        assert g[halfway_idx] == halfway_idx
        # These are now concretised so they should be returned without any work
        assert g[halfway_idx] == halfway_idx
        for i in range(halfway_idx + 1):
            assert g[i] == i
        # This should only need to concretise one more element
        assert g[halfway_idx + 1] == halfway_idx + 1

    def test_done_does_not_lag(self, g):
        """
        Don't allow regen to return from `__iter__()` and check `__done`.
        """
        # The range we zip with here should ensure that the `regen.__iter__`
        # call never gets to return since we never attempt a failing `next()`
        len_g = g.__length_hint__()
        for i, __ in zip(range(len_g), g):
            assert getattr(g, "_regen__done") is (i == len_g - 1)
        # Just for sanity, check against a specific `bool` here
        assert getattr(g, "_regen__done") is True

    def test_lookahead_consume(self, subtests):
        """
        Confirm that regen looks ahead by a single item as expected.
        """
        def g():
            yield from ["foo", "bar"]
            raise pytest.fail("This should never be reached")

        with subtests.test(msg="bool does not overconsume"):
            assert bool(regen(g()))
        with subtests.test(msg="getitem 0th does not overconsume"):
            assert regen(g())[0] == "foo"
        with subtests.test(msg="single iter does not overconsume"):
            assert next(iter(regen(g()))) == "foo"

        class ExpectedException(BaseException):
            pass

        def g2():
            yield from ["foo", "bar"]
            raise ExpectedException()

        with subtests.test(msg="getitem 1th does overconsume"):
            r = regen(g2())
            with pytest.raises(ExpectedException):
                r[1]
            # Confirm that the item was concretised anyway
            assert r[1] == "bar"
        with subtests.test(msg="full iter does overconsume"):
            r = regen(g2())
            with pytest.raises(ExpectedException):
                for _ in r:
                    pass
            # Confirm that the items were concretised anyway
            assert r == ["foo", "bar"]
        with subtests.test(msg="data access does overconsume"):
            r = regen(g2())
            with pytest.raises(ExpectedException):
                r.data
            # Confirm that the items were concretised anyway
            assert r == ["foo", "bar"]


class test_head_from_fun:

    def test_from_cls(self):
        class X:
            def __call__(x, y, kwarg=1):
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

    def test_kwonly_required_args(self):
        local = {}
        fun = ('def f_kwargs_required(*, a="a", b, c=None):'
               '    return')
        exec(fun, {}, local)
        f_kwargs_required = local['f_kwargs_required']
        g = head_from_fun(f_kwargs_required)

        with pytest.raises(TypeError):
            g(1)

        with pytest.raises(TypeError):
            g(a=1)

        with pytest.raises(TypeError):
            g(c=1)

        with pytest.raises(TypeError):
            g(a=2, c=1)

        g(b=3)


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
    assert type(res) is type(expected)
    assert res == expected


@pytest.mark.parametrize('a,b,expected', [
    ((1, 2, 3), 4, (1, 2, 3, 4)),
    ([1, 2, 3], 4, [1, 2, 3, 4]),
])
def test_seq_concat_item(a, b, expected):
    res = seq_concat_item(a, b)
    assert type(res) is type(expected)
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
