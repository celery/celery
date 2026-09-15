import asyncio
import threading
import time

import pytest

from celery._state import _task_stack
from celery.concurrency.asyncio import TaskPool
from celery.exceptions import SoftTimeLimitExceeded, Terminated, TimeLimitExceeded
from celery.utils.coroutines import get_coroutine_runner, resolve_coroutine


@pytest.fixture
def pool():
    p = TaskPool(limit=4)
    p.start()
    try:
        yield p
    finally:
        p.stop()


class test_TaskPool:

    def test_start_installs_runner_and_loop(self, pool):
        assert pool.active
        assert get_coroutine_runner() == pool.run_coroutine
        assert pool._loop_thread.is_alive()

    def test_stop_restores_previous_runner(self):
        p = TaskPool(limit=1)
        p.start()
        assert get_coroutine_runner() is not None
        p.stop()
        assert get_coroutine_runner() is None
        assert p._loop_thread is None
        assert p._executor is None

    def test_two_pools_stopped_out_of_order_do_not_clobber_each_other(self):
        """Nothing forbids two TaskPool instances in one process; stopping
        the one started first must not evict a still-running second pool's
        runner (regression: a single saved "previous runner" slot did)."""
        p1, p2 = TaskPool(limit=1), TaskPool(limit=1)
        p1.start()
        p2.start()
        assert get_coroutine_runner() == p2.run_coroutine
        try:
            p1.stop()  # non-LIFO: started first, stopped first
            assert get_coroutine_runner() == p2.run_coroutine

            async def where():
                return id(asyncio.get_running_loop())

            # p2 must still actually work, on its own loop.
            assert resolve_coroutine(where()) == id(p2._loop)
        finally:
            p2.stop()
        assert get_coroutine_runner() is None

    def test_info(self, pool):
        info = pool.info
        assert info['max-concurrency'] == 4
        assert info['running-coroutines'] == 0
        assert info['implementation'].endswith('asyncio:TaskPool')

    def test_apply_runs_synchronous_target(self, pool):
        results = []
        accepted = []
        pool.apply_async(
            lambda x: x * 2, (21,),
            callback=results.append,
            accept_callback=lambda pid, t: accepted.append(pid),
        ).wait(5)
        assert results == [42]
        # the reported pid is the tracer thread's id
        assert accepted and accepted[0] != threading.get_ident()

    def test_coroutines_share_one_loop(self, pool):
        async def which_loop():
            return id(asyncio.get_running_loop())

        loops = {resolve_coroutine(which_loop()) for _ in range(3)}
        assert len(loops) == 1
        assert loops.pop() == id(pool._loop)

    def test_coroutine_result_and_exception(self, pool):
        async def ok():
            await asyncio.sleep(0)
            return 'value'

        async def boom():
            raise KeyError('nope')

        assert resolve_coroutine(ok()) == 'value'
        with pytest.raises(KeyError):
            resolve_coroutine(boom())

    def test_concurrency_is_real(self, pool):
        started = threading.Barrier(3, timeout=5)

        async def slow():
            await asyncio.sleep(0.2)
            return 1

        results = []
        handles = [
            pool.apply_async(
                lambda: (started.wait(), resolve_coroutine(slow()))[1],
                callback=results.append,
            )
            for _ in range(3)
        ]
        start = time.monotonic()
        for handle in handles:
            handle.wait(5)
        elapsed = time.monotonic() - start
        assert results == [1, 1, 1]
        # Serial execution would take at least 0.6s.
        assert elapsed < 0.5

    def test_refuses_to_run_from_the_loop_thread(self, pool):
        async def outer():
            async def inner():
                return 1
            return pool.run_coroutine(inner())

        with pytest.raises(RuntimeError, match='deadlock'):
            resolve_coroutine(outer())

    def test_run_coroutine_when_pool_is_down(self):
        """Reported as a terminated task, not as a misconfigured worker."""
        p = TaskPool(limit=1)
        p.start()
        p.stop()

        async def noop():
            return None

        with pytest.raises(Terminated, match='shutting down'):
            p.run_coroutine(noop())

    def test_apply_when_pool_is_down(self):
        p = TaskPool(limit=1)
        with pytest.raises(RuntimeError, match='not running'):
            p.apply_async(lambda: None)


class test_time_limits:

    def _run_with_limits(self, pool, coro, timeout=None, soft_timeout=None,
                         timeout_callback=None):
        """Run ``coro`` the way a traced task would, with time limits set."""
        out = {}

        def body():
            try:
                out['result'] = resolve_coroutine(coro)
            except BaseException as exc:
                out['exc'] = exc

        pool.apply_async(
            body, timeout=timeout, soft_timeout=soft_timeout,
            timeout_callback=timeout_callback,
        ).wait(5)
        return out

    def test_soft_time_limit_cancels_and_reports(self, pool):
        warned = []

        async def forever():
            await asyncio.sleep(30)

        out = self._run_with_limits(
            pool, forever(), soft_timeout=0.1,
            timeout_callback=lambda **kw: warned.append(kw),
        )
        assert isinstance(out['exc'], SoftTimeLimitExceeded)
        assert warned == [{'soft': True, 'timeout': 0.1}]

    def test_task_may_catch_the_soft_limit(self, pool):
        async def cleans_up():
            try:
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                return 'cleaned up'

        out = self._run_with_limits(pool, cleans_up(), soft_timeout=0.1)
        assert out['result'] == 'cleaned up'

    def test_hard_time_limit(self, pool):
        async def forever():
            await asyncio.sleep(30)

        out = self._run_with_limits(pool, forever(), timeout=0.1)
        assert isinstance(out['exc'], TimeLimitExceeded)

    def test_hard_limit_after_ignored_soft_limit(self, pool):
        async def stubborn():
            while True:
                try:
                    await asyncio.sleep(30)
                except asyncio.CancelledError:
                    pass

        out = self._run_with_limits(
            pool, stubborn(), soft_timeout=0.1, timeout=0.3)
        assert isinstance(out['exc'], TimeLimitExceeded)

    def test_no_limits_returns_normally(self, pool):
        async def quick():
            return 'done'

        out = self._run_with_limits(pool, quick())
        assert out['result'] == 'done'


class test_terminate_job:

    def test_cancels_the_running_coroutine(self, pool):
        running = threading.Event()
        out = {}
        accepted = []

        async def forever():
            running.set()
            await asyncio.sleep(30)

        def body():
            try:
                out['result'] = resolve_coroutine(forever())
            except BaseException as exc:
                out['exc'] = exc

        handle = pool.apply_async(
            body, accept_callback=lambda pid, t: accepted.append(pid))
        assert running.wait(5)
        pool.terminate_job(accepted[0])
        handle.wait(5)
        assert isinstance(out['exc'], Terminated)

    def test_job_ids_are_never_reused(self, pool):
        """A thread id would be: the executor hands the same thread out again."""
        seen = []
        for _ in range(3):
            pool.apply_async(
                lambda: None,
                accept_callback=lambda pid, t: seen.append(pid)).wait(5)
        assert len(set(seen)) == 3

    def test_unknown_job_is_a_noop(self, pool, caplog):
        pool.terminate_job(-1)
        with caplog.at_level("DEBUG"):
            pool.terminate_job(-1)
        assert "no job -1 to terminate" in caplog.text

    def test_terminate_before_the_body_starts(self, pool):
        """A revoke can land while the tracer is still decoding the message."""
        out = {}
        started = threading.Event()

        async def forever():
            await asyncio.sleep(30)

        def body():
            started.wait(5)               # stand in for the tracer prologue
            try:
                out['result'] = resolve_coroutine(forever())
            except BaseException as exc:
                out['exc'] = exc

        accepted = []
        handle = pool.apply_async(
            body, accept_callback=lambda pid, t: accepted.append(pid))
        while not accepted:
            time.sleep(0.01)
        pool.terminate_job(accepted[0])   # nothing to cancel yet
        started.set()
        handle.wait(5)
        assert isinstance(out['exc'], Terminated)

    def test_terminate_while_queued_does_not_drop_the_job(self):
        """A queued job must not vanish: it runs, then stops at its body."""
        p = TaskPool(limit=1)
        p.start()
        blocked = threading.Event()
        out = {}

        async def forever():
            await asyncio.sleep(30)

        def body():
            out['ran'] = True
            try:
                resolve_coroutine(forever())
            except BaseException as exc:
                out['exc'] = exc

        try:
            p.apply_async(blocked.wait, (5,))
            handle = p.apply_async(body)
            handle.terminate()                # still queued behind the block
            blocked.set()
            handle.wait(5)
            assert out.get('ran') is True
            assert isinstance(out['exc'], Terminated)
        finally:
            blocked.set()
            p.stop()


class test_shutdown:

    def test_terminate_cancels_in_flight_coroutines(self):
        p = TaskPool(limit=2)
        p.start()
        running = threading.Event()
        out = {}

        async def forever():
            running.set()
            await asyncio.sleep(30)

        def body():
            try:
                out['result'] = resolve_coroutine(forever())
            except BaseException as exc:
                out['exc'] = exc

        p.apply_async(body)
        assert running.wait(5)
        p.terminate()
        for _ in range(50):
            if 'exc' in out:
                break
            time.sleep(0.05)
        assert isinstance(out['exc'], Terminated)
        assert get_coroutine_runner() is None

    def test_stop_is_idempotent(self):
        p = TaskPool(limit=1)
        p.start()
        p.stop()
        p.stop()
        assert get_coroutine_runner() is None


class test_signals:

    def test_worker_process_init_is_sent_on_start(self):
        from celery import signals

        received = []

        def on_init(**kwargs):
            received.append(kwargs)

        # weak=False: the local handler would otherwise be collected at once.
        signals.worker_process_init.connect(on_init, weak=False)
        p = TaskPool(limit=1)
        p.start()
        try:
            assert len(received) == 1
        finally:
            signals.worker_process_init.disconnect(on_init)
            p.stop()


class test_request_context:
    """`self.request` must survive the hop onto the loop thread."""

    def _fake_task(self, name):
        from celery.app.task import Context
        from celery.utils.threads import LocalStack

        class FakeTask:
            """Shaped like Task: `request` reads the top of the stack."""

            def __init__(self):
                self.request_stack = LocalStack()

            @property
            def request(self):
                return self.request_stack.top

        return FakeTask(), Context(id=name)

    def _trace(self, pool, task, request, coro_factory):
        """Run a coroutine the way the tracer would, context and all."""
        out = {}

        def body():
            _task_stack.push(task)
            task.request_stack.push(request)
            try:
                out['result'] = resolve_coroutine(coro_factory())
            except BaseException as exc:  # pragma: no cover
                out['exc'] = exc
            finally:
                _task_stack.pop()
                task.request_stack.pop()

        return body, out

    def test_request_is_visible_inside_the_coroutine(self, pool):
        task, request = self._fake_task('task-1')

        async def read_request():
            before = _task_stack.top.request_stack.top.id
            await asyncio.sleep(0)
            return before, _task_stack.top.request_stack.top.id

        body, out = self._trace(pool, task, request, read_request)
        pool.apply_async(body).wait(5)
        assert out['result'] == ('task-1', 'task-1')

    def test_concurrent_coroutines_keep_their_own_request(self, pool):
        seen = {}
        started = threading.Barrier(3, timeout=5)

        def make(name):
            task, request = self._fake_task(name)

            async def read_request():
                await asyncio.sleep(0.05)
                return _task_stack.top.request_stack.top.id

            body, out = self._trace(pool, task, request, read_request)

            def run():
                started.wait()
                body()
                seen[name] = out['result']

            return run

        handles = [pool.apply_async(make(f'task-{i}')) for i in range(3)]
        for handle in handles:
            handle.wait(5)
        assert seen == {'task-0': 'task-0', 'task-1': 'task-1',
                        'task-2': 'task-2'}

    def test_no_context_is_not_an_error(self, pool):
        async def noop():
            return 'fine'

        assert resolve_coroutine(noop()) == 'fine'


class test_lifecycle_signals:

    def test_process_init_and_shutdown_are_sent(self):
        from celery import signals

        seen = []

        def on_init(**kwargs):
            seen.append('init')

        def on_shutdown(**kwargs):
            seen.append('shutdown')

        signals.worker_process_init.connect(on_init, weak=False)
        signals.worker_process_shutdown.connect(on_shutdown, weak=False)
        p = TaskPool(limit=1)
        try:
            p.start()
            p.stop()
            assert seen == ['init', 'shutdown']
        finally:
            signals.worker_process_init.disconnect(on_init)
            signals.worker_process_shutdown.disconnect(on_shutdown)

    def test_unsupported_options_warn(self, caplog):
        p = TaskPool(limit=1, maxtasksperchild=100)
        p.start()
        p.stop()
        assert 'maxtasksperchild has no effect' in caplog.text


class test_abandoned_bodies:
    """A body that swallows its cancellation must not leak its context."""

    def test_storage_is_released(self, pool):
        from celery._state import _task_stack

        task, request = test_request_context()._fake_task('zombie')
        out = {}

        async def stubborn():
            while True:
                try:
                    await asyncio.sleep(30)
                except asyncio.CancelledError:
                    pass

        def body():
            _task_stack.push(task)
            task.request_stack.push(request)
            try:
                out['result'] = resolve_coroutine(stubborn())
            except BaseException as exc:
                out['exc'] = exc
            finally:
                _task_stack.pop()
                task.request_stack.pop()

        pool.apply_async(body, soft_timeout=0.1).wait(10)
        assert isinstance(out['exc'], SoftTimeLimitExceeded)
        # The zombie is still on the loop, but nothing of its request is left.
        assert not task.request_stack._local.__storage__
        assert not [key for key in _task_stack._local.__storage__
                    if not isinstance(key, int)]
