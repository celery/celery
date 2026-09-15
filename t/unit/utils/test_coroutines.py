import asyncio

import pytest

from celery.exceptions import ImproperlyConfigured
from celery.utils.coroutines import (default_coroutine_runner, get_coroutine_runner, pop_coroutine_runner,
                                     push_coroutine_runner, resolve_coroutine, set_coroutine_runner)


@pytest.fixture(autouse=True)
def restore_runner():
    previous = get_coroutine_runner()
    try:
        yield
    finally:
        set_coroutine_runner(previous)


async def answer():
    await asyncio.sleep(0)
    return 42


async def raises():
    raise KeyError('boom')


class test_default_runner:

    def test_runs_the_coroutine(self):
        assert default_coroutine_runner(answer()) == 42

    def test_propagates_exceptions(self):
        with pytest.raises(KeyError):
            default_coroutine_runner(raises())

    def test_used_when_no_runner_is_installed(self):
        set_coroutine_runner(None)
        assert get_coroutine_runner() is None
        assert resolve_coroutine(answer(), fallback=True) == 42

    def test_refuses_without_a_runner_unless_asked(self):
        set_coroutine_runner(None)
        with pytest.raises(ImproperlyConfigured, match='--pool=asyncio'):
            resolve_coroutine(answer())

    def test_already_inside_a_running_loop(self):
        set_coroutine_runner(None)

        async def outer():
            return resolve_coroutine(answer(), fallback=True)

        # asyncio.run() would refuse; the runner gives it its own thread.
        assert asyncio.run(outer()) == 42

    def test_each_call_gets_its_own_loop(self):
        # Kept alive in a list, so the second loop cannot be allocated at the
        # address the first one was freed from.
        loops = []

        async def grab_loop():
            loops.append(asyncio.get_running_loop())

        resolve_coroutine(grab_loop(), fallback=True)
        resolve_coroutine(grab_loop(), fallback=True)
        assert loops[0] is not loops[1]


class test_installed_runner:

    def test_set_returns_the_previous_runner(self):
        first = set_coroutine_runner(None)  # noqa: F841
        marker = object()

        def runner(coro):
            coro.close()
            return marker

        assert set_coroutine_runner(runner) is None
        assert get_coroutine_runner() is runner
        assert set_coroutine_runner(None) is runner

    def test_resolve_delegates_to_the_installed_runner(self):
        seen = []

        def runner(coro):
            seen.append(coro)
            return default_coroutine_runner(coro)

        set_coroutine_runner(runner)
        assert resolve_coroutine(answer()) == 42
        assert len(seen) == 1


class test_pushed_runners:
    """Two pool instances alive at once must not steal each other's runner."""

    def test_stopping_the_first_pool_does_not_evict_the_second(self):
        first, second = object(), object()
        token1 = push_coroutine_runner(first)
        token2 = push_coroutine_runner(second)
        assert get_coroutine_runner() is second

        # Non-LIFO on purpose: the pool started first is stopped first.
        pop_coroutine_runner(token1)
        assert get_coroutine_runner() is second

        pop_coroutine_runner(token2)
        assert get_coroutine_runner() is None

    def test_stopping_in_lifo_order_also_works(self):
        first, second = object(), object()
        token1 = push_coroutine_runner(first)
        token2 = push_coroutine_runner(second)

        pop_coroutine_runner(token2)
        assert get_coroutine_runner() is first

        pop_coroutine_runner(token1)
        assert get_coroutine_runner() is None

    def test_popping_twice_is_a_noop(self):
        token = push_coroutine_runner(object())
        pop_coroutine_runner(token)
        pop_coroutine_runner(token)  # must not raise
        assert get_coroutine_runner() is None

    def test_set_coroutine_runner_replaces_the_whole_stack(self):
        """The simple single-slot API, for a caller that wants just one."""
        push_coroutine_runner(object())
        push_coroutine_runner(object())
        marker = object()
        set_coroutine_runner(marker)
        assert get_coroutine_runner() is marker
        set_coroutine_runner(None)
        assert get_coroutine_runner() is None
