import operator
import threading
import time
from concurrent.futures import Future

import pytest

import t.skip
from celery.concurrency import thread
from celery.exceptions import Terminated
from celery.utils.functional import noop
from celery.worker import state


class test_thread_TaskPool:

    def test_on_apply(self):
        from celery.concurrency import thread
        x = thread.TaskPool()
        try:
            x.on_apply(operator.add, (2, 2), {}, noop, noop)
        finally:
            x.stop()

    def test_info(self):
        from celery.concurrency import thread
        x = thread.TaskPool()
        try:
            assert x.info
        finally:
            x.stop()

    def test_on_stop(self):
        from celery.concurrency import thread
        x = thread.TaskPool()
        x.on_stop()
        with pytest.raises(RuntimeError):
            x.on_apply(operator.add, (2, 2), {}, noop, noop)

    def test_on_stop_cancels_pending_futures(self):
        import threading

        from celery.concurrency import thread

        x = thread.TaskPool(limit=1)

        started = threading.Event()
        shutdown = threading.Event()

        def blocking_task():
            started.set()
            shutdown.wait(timeout=5)

        stop_thread = None
        try:
            # Submit a long-running task to occupy the single thread
            x.on_apply(blocking_task, (), {}, noop, noop)

            # Wait until the first task is actually running
            assert started.wait(timeout=5), "Timed out waiting for blocking_task to start"

            # Submit another task — guaranteed to be pending
            result = x.on_apply(noop, (), {}, noop, noop)

            # Stop the pool in a background thread — should cancel the pending future
            def _run_on_stop():
                x.on_stop()

            stop_thread = threading.Thread(target=_run_on_stop)
            stop_thread.start()

            # Wait (bounded) until the pending future has been cancelled
            deadline = time.time() + 5.0
            while not result.f.cancelled() and time.time() < deadline:
                time.sleep(0.01)

            if not result.f.cancelled():
                pytest.fail("Pending futures should be cancelled on stop")

            # Once cancellation is observed, release the blocking thread so on_stop can finish
            shutdown.set()
            if stop_thread is not None:
                stop_thread.join(timeout=5.0)
        finally:
            # Release the blocking thread and ensure pool is stopped
            # even if the test fails, preventing thread leaks.
            # on_stop() is idempotent — safe to call twice.
            shutdown.set()
            if stop_thread is not None and stop_thread.is_alive():
                stop_thread.join(timeout=5.0)
            x.on_stop()

    def test_terminate_job_ignores_finished_task(self):
        x = thread.TaskPool(limit=1)
        pids, results = [], []
        try:
            x.on_apply(noop, (), {}, noop,
                       lambda pid, _: pids.append(pid)).wait(timeout=5)
            x.terminate_job(pids[0], signal=15)

            x.on_apply(operator.add, (2, 2), {},
                       results.append, noop).wait(timeout=5)
            assert results == [4]
        finally:
            x.stop()

    @t.skip.if_pypy
    def test_terminate_job_interrupts_task(self):
        x = thread.TaskPool(limit=1)
        started = threading.Event()
        pids, outcome = [], []

        def spinning_task():
            started.set()
            deadline = time.monotonic() + 10.0
            try:
                while time.monotonic() < deadline:
                    pass
            except Terminated:
                outcome.append('terminated')
                raise

        try:
            result = x.on_apply(spinning_task, (), {}, noop,
                                lambda pid, _: pids.append(pid))
            assert started.wait(timeout=5), 'spinning_task did not start'
            x.terminate_job(pids[0], signal=15)
            result.wait(timeout=5)
            assert outcome == ['terminated']
        finally:
            x.executor.shutdown(wait=True)

    def test_terminate_cancels_pending_future(self):
        result = thread.ApplyResult(Future())
        result.terminate(signal=15)
        assert result.f.cancelled()

    def test_on_stop_waits_for_running_tasks(self, monkeypatch):
        monkeypatch.setattr(state, 'should_terminate', None)
        x = thread.TaskPool(limit=1)
        started = threading.Event()
        finished = threading.Event()

        def blocking_task():
            started.set()
            time.sleep(0.5)
            finished.set()

        x.on_apply(blocking_task, (), {}, noop, noop)
        assert started.wait(timeout=5), "Timed out waiting for blocking_task to start"
        x.on_stop()
        assert finished.is_set()

    def test_on_stop_does_not_wait_on_cold_shutdown(self, monkeypatch):
        x = thread.TaskPool(limit=1)
        started = threading.Event()
        release = threading.Event()

        def blocking_task():
            started.set()
            release.wait(timeout=30)

        try:
            x.on_apply(blocking_task, (), {}, noop, noop)
            assert started.wait(timeout=5), "Timed out waiting for blocking_task to start"
            monkeypatch.setattr(state, 'should_terminate', True)

            t0 = time.monotonic()
            x.on_stop()
            assert time.monotonic() - t0 < 5.0
            assert not release.is_set()
        finally:
            release.set()
            x.executor.shutdown(wait=True)

    def test_on_terminate_cancels_pending(self):
        x = thread.TaskPool(limit=1)
        started = threading.Event()
        release = threading.Event()

        def blocking_task():
            started.set()
            release.wait(timeout=30)

        try:
            x.on_apply(blocking_task, (), {}, noop, noop)
            assert started.wait(timeout=5), "Timed out waiting for blocking_task to start"
            pending = x.on_apply(noop, (), {}, noop, noop)

            t0 = time.monotonic()
            x.terminate()
            assert time.monotonic() - t0 < 5.0
            assert pending.f.cancelled()
            assert not release.is_set()
        finally:
            release.set()
            x.executor.shutdown(wait=True)
