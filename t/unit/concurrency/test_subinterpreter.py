import gc
import os
import sys
import threading
import time
import weakref
from unittest.mock import Mock, patch

import pytest
from billiard.exceptions import WorkerLostError

interpreters = pytest.importorskip('concurrent.interpreters')

from celery.app.trace import trace_task_ret  # noqa: E402
from celery.concurrency import subinterpreter  # noqa: E402

from . import subinterpreter_helpers as helpers  # noqa: E402


@pytest.fixture
def pool():
    x = subinterpreter.TaskPool(limit=2)
    x.start()
    try:
        yield x
    finally:
        x.stop()


class test_subinterpreter_TaskPool:

    def test_on_apply_returns_result(self, pool):
        assert pool.on_apply(pow, (2, 10)).get(timeout=10) == 1024

    def test_runs_in_subinterpreter(self, pool):
        main_id = interpreters.get_current().id
        res = pool.on_apply(helpers.current_interpreter_id).get(timeout=10)
        assert res != main_id

    def test_callbacks_run_in_main_interpreter(self, pool):
        calls = []

        def accept_callback(pid, time_accepted):
            calls.append(('accepted', interpreters.get_current().id, pid))

        def callback(ret):
            calls.append(('done', interpreters.get_current().id, ret))

        pool.on_apply(pow, (2, 3), {}, callback, accept_callback).wait(10)
        main_id = interpreters.get_current().id
        assert calls == [
            ('accepted', main_id, os.getpid()),
            ('done', main_id, 8),
        ]

    def test_sys_path_is_copied(self):
        sys.path.append('/added/by/main')
        try:
            x = subinterpreter.TaskPool(limit=1)
            x.start()
        finally:
            sys.path.remove('/added/by/main')
        try:
            res = x.on_apply(helpers.get_sys_path).get(timeout=10)
            assert '/added/by/main' in res
        finally:
            x.stop()

    def test_failure_calls_error_callback(self, pool):
        callback, error_callback = Mock(), Mock()
        pool.on_apply(helpers.fail, (), {}, callback,
                      error_callback=error_callback).wait(10)
        callback.assert_not_called()
        exc = error_callback.call_args[0][0].exception.exc
        assert isinstance(exc, WorkerLostError)
        assert 'nope' in str(exc)

    def test_failure_without_error_callback_raises(self, pool):
        with pytest.raises(interpreters.ExecutionFailed):
            pool.on_apply(helpers.fail).get(timeout=10)

    def test_interpreter_reused_after_failure(self):
        x = subinterpreter.TaskPool(limit=1)
        x.start()
        try:
            with pytest.raises(interpreters.ExecutionFailed):
                x.on_apply(helpers.fail).get(timeout=10)
            assert x.on_apply(pow, (2, 2)).get(timeout=10) == 4
        finally:
            x.stop()

    def test_initializer_sets_up_worker_app(self):
        x = subinterpreter.TaskPool(limit=1,
                                    initargs=(helpers.app, 'w1@example.com'))
        x.start()
        try:
            request = {'id': 'id1', 'task': helpers.add.name,
                       'delivery_info': {}}
            failed, retval, _ = x.on_apply(
                trace_task_ret,
                (helpers.add.name, 'id1', request, ((2, 3), {}, None),
                 None, None),
            ).get(timeout=30)
            assert (failed, retval) == (0, '5')
        finally:
            x.stop()

    def test_start_closes_created_interpreters_on_failure(self):
        x = subinterpreter.TaskPool(limit=3)
        x.initializer = helpers.fail_setup_in_some
        before = len(interpreters.list_all())
        try:
            with pytest.raises(interpreters.ExecutionFailed):
                x.start()
            assert len(interpreters.list_all()) == before
            assert x.info['interpreters'] == 0
        finally:
            x.stop()

    def test_start_fails_if_setup_fails(self):
        x = subinterpreter.TaskPool(limit=2)
        x.initializer = helpers.fail_setup
        before = len(interpreters.list_all())
        try:
            with patch.object(subinterpreter.logger, 'critical') as critical:
                with pytest.raises(interpreters.ExecutionFailed):
                    x.start()
            critical.assert_called_once()
            assert len(interpreters.list_all()) == before
        finally:
            x.stop()

    def test_default_limit(self):
        x = subinterpreter.TaskPool()
        try:
            assert x.limit == (os.process_cpu_count() or 1)
        finally:
            x.stop()

    def test_info(self, pool):
        info = pool.info
        assert info['max-concurrency'] == 2
        assert info['max-tasks-per-child'] == 'N/A'
        assert info['interpreters'] == 2

    def test_hard_time_limit_warns(self):
        x = subinterpreter.TaskPool(limit=1, timeout=10)
        with patch.object(subinterpreter.logger, 'warning') as warning:
            x.start()
        try:
            warning.assert_called_once()
        finally:
            x.stop()

    def test_terminate_job_does_nothing(self, pool):
        with patch.object(subinterpreter.logger, 'warning') as warning:
            pool.terminate_job(os.getpid(), 15)
        warning.assert_not_called()

    def test_terminate_cancels_pending_job(self):
        x = subinterpreter.TaskPool(limit=1)
        x.start()
        started = threading.Event()
        try:
            running = x.on_apply(time.sleep, (0.5,), {}, None,
                                 lambda *_: started.set())
            assert started.wait(timeout=10)
            pending = x.on_apply(pow, (2, 2))
            running.terminate(15)
            pending.terminate(15)
            assert running.get(timeout=10) is None
            assert pending.f.cancelled()
        finally:
            x.stop()

    def test_on_stop(self):
        x = subinterpreter.TaskPool(limit=1)
        x.start()
        x.on_stop()
        assert x.info['interpreters'] == 0
        with pytest.raises(RuntimeError):
            x.on_apply(pow, (2, 2))

    def test_on_stop_cancels_pending(self):
        x = subinterpreter.TaskPool(limit=1)
        x.start()
        started = threading.Event()
        try:
            running = x.on_apply(time.sleep, (0.5,), {}, None,
                                 lambda *_: started.set())
            assert started.wait(timeout=10)
            pending = x.on_apply(pow, (2, 2))
            x.on_stop()
            assert running.f.done() and not running.f.cancelled()
            assert pending.f.cancelled()
        finally:
            x.stop()


class test_stop_requested:

    def test_false_outside_pool(self):
        assert subinterpreter.stop_requested() is False

    def test_false_by_default(self, pool):
        assert pool.on_apply(helpers.is_stop_requested).get(timeout=10) is False

    def test_terminate_running_job_requests_stop(self, pool):
        started = threading.Event()
        res = pool.on_apply(helpers.wait_for_stop, (10,), {}, None,
                            lambda *_: started.set())
        assert started.wait(timeout=10)
        with patch.object(subinterpreter.logger, 'warning') as warning:
            res.terminate(15)
        warning.assert_called_once()
        assert res.get(timeout=10) is True

    def test_result_kept_alive_until_done(self, pool):
        # Requests only keep a weak reference to the result.
        started = threading.Event()
        ref = weakref.ref(pool.on_apply(helpers.wait_for_stop, (10,), {},
                                        None, lambda *_: started.set()))
        assert started.wait(timeout=10)
        gc.collect()
        res = ref()
        assert res is not None
        res.terminate(15)
        res.wait(10)
        del res
        deadline = time.monotonic() + 10
        while ref() is not None and time.monotonic() < deadline:
            gc.collect()
            time.sleep(0.01)
        assert ref() is None

    def test_request_before_start_is_delivered(self):
        job = subinterpreter._Job()
        job.request_stop()
        interp = Mock()
        job.start(interp)
        interp.clear_stop.assert_called_once_with()
        interp.request_stop.assert_called_once_with()

    def test_request_after_done_is_ignored(self):
        job = subinterpreter._Job()
        interp = Mock()
        job.start(interp)
        job.done()
        job.request_stop()
        interp.request_stop.assert_not_called()

    def test_stale_request_cleared_for_next_job(self):
        x = subinterpreter.TaskPool(limit=1)
        x.start()
        try:
            res = x.on_apply(helpers.is_stop_requested)
            assert res.get(timeout=10) is False
            # the job is done, the request reaches no task.
            res.terminate(15)
            x._interpreters[0].request_stop()
            assert x.on_apply(helpers.is_stop_requested).get(timeout=10) is False
        finally:
            x.stop()

    def test_soft_time_limit_requests_stop(self, pool):
        with patch.object(subinterpreter.logger, 'warning') as warning:
            res = pool.on_apply(helpers.wait_for_stop, (10,), soft_timeout=0.1)
            assert res.get(timeout=10) is True
        warning.assert_called_once()

    def test_soft_time_limit_not_reached(self, pool):
        res = pool.on_apply(helpers.wait_for_stop, (0.1,), soft_timeout=10)
        assert res.get(timeout=10) is False


class test_max_tasks_per_child:

    def test_interpreter_replaced(self):
        x = subinterpreter.TaskPool(limit=1, maxtasksperchild=2)
        x.start()
        before = len(interpreters.list_all())
        try:
            ids = [x.on_apply(helpers.current_interpreter_id).get(timeout=10)
                   for _ in range(5)]
            assert ids[0] == ids[1] != ids[2] == ids[3] != ids[4]
            assert x.info['interpreters'] == 1
            assert x.info['max-tasks-per-child'] == 2
            assert len(interpreters.list_all()) == before
        finally:
            x.stop()

    def test_old_interpreter_shutdown_signal(self):
        x = subinterpreter.TaskPool(limit=1, maxtasksperchild=1)
        x.start()
        try:
            old = x._interpreters[0]
            with patch.object(old, 'close', wraps=old.close) as close:
                x.on_apply(pow, (2, 2)).get(timeout=10)
                deadline = time.monotonic() + 10
                while not close.called and time.monotonic() < deadline:
                    time.sleep(0.01)
            close.assert_called_once_with()
            assert x._interpreters[0] is not old
        finally:
            x.stop()

    def test_keeps_interpreter_if_replacement_fails(self):
        x = subinterpreter.TaskPool(limit=1, maxtasksperchild=1)
        x.start()
        try:
            old = x._interpreters[0]
            with patch.object(x, '_create_interpreter',
                              side_effect=RuntimeError('boom')), \
                    patch.object(subinterpreter.logger, 'exception') as log:
                assert x.on_apply(pow, (2, 2)).get(timeout=10) == 4
                assert x.on_apply(pow, (2, 3)).get(timeout=10) == 8
            assert log.call_count == 2
            assert x._interpreters == [old]
        finally:
            x.stop()


class test_on_terminate:

    def test_running_interpreter_left_open(self):
        x = subinterpreter.TaskPool(limit=2)
        x.start()
        started = threading.Event()
        before = len(interpreters.list_all())
        res = x.on_apply(time.sleep, (0.5,), {}, None,
                         lambda *_: started.set())
        assert started.wait(timeout=10)
        with patch.object(subinterpreter.logger, 'warning') as warning:
            x.terminate()
        warning.assert_called_once()
        assert len(interpreters.list_all()) == before - 1
        res.wait(10)
