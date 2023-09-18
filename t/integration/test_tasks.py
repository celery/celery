import logging
import time
from datetime import datetime, timedelta
from time import perf_counter, sleep
from uuid import uuid4

import pytest

import celery
from celery import chain, chord, group
from celery.canvas import StampingVisitor
from celery.signals import task_received
from celery.utils.serialization import UnpickleableExceptionWrapper
from celery.worker import state as worker_state

from .conftest import TEST_BACKEND, get_active_redis_channels, get_redis_connection
from .tasks import (ClassBasedAutoRetryTask, ExpectedException, add, add_ignore_result, add_not_typed, fail,
                    fail_unpickleable, print_unicode, retry, retry_once, retry_once_headers, retry_once_priority,
                    retry_unpickleable, return_properties, second_order_replace1, sleeping)

TIMEOUT = 10


_flaky = pytest.mark.flaky(reruns=5, reruns_delay=2)
_timeout = pytest.mark.timeout(timeout=300)


def flaky(fn):
    return _timeout(_flaky(fn))


class test_class_based_tasks:

    @flaky
    def test_class_based_task_retried(self, celery_session_app,
                                      celery_session_worker):
        task = ClassBasedAutoRetryTask()
        celery_session_app.register_task(task)
        res = task.delay()
        assert res.get(timeout=TIMEOUT) == 1


def _producer(j):
    """Single producer helper function"""
    results = []
    for i in range(20):
        results.append([i + j, add.delay(i, j)])
    for expected, result in results:
        value = result.get(timeout=10)
        assert value == expected
        assert result.status == 'SUCCESS'
        assert result.ready() is True
        assert result.successful() is True
    return j


class test_tasks:

    def test_simple_call(self):
        """Tests direct simple call of task"""
        assert add(1, 1) == 2
        assert add(1, 1, z=1) == 3

    @flaky
    def test_basic_task(self, manager):
        """Tests basic task call"""
        results = []
        # Tests calling task only with args
        for i in range(10):
            results.append([i + i, add.delay(i, i)])
        for expected, result in results:
            value = result.get(timeout=10)
            assert value == expected
            assert result.status == 'SUCCESS'
            assert result.ready() is True
            assert result.successful() is True

        results = []
        # Tests calling task with args and kwargs
        for i in range(10):
            results.append([3*i, add.delay(i, i, z=i)])
        for expected, result in results:
            value = result.get(timeout=10)
            assert value == expected
            assert result.status == 'SUCCESS'
            assert result.ready() is True
            assert result.successful() is True

    @flaky
    def test_multiprocess_producer(self, manager):
        """Testing multiple processes calling tasks."""
        from multiprocessing import Pool
        pool = Pool(20)
        ret = pool.map(_producer, range(120))
        assert list(ret) == list(range(120))

    @flaky
    def test_multithread_producer(self, manager):
        """Testing multiple threads calling tasks."""
        from multiprocessing.pool import ThreadPool
        pool = ThreadPool(20)
        ret = pool.map(_producer, range(120))
        assert list(ret) == list(range(120))

    @flaky
    def test_ignore_result(self, manager):
        """Testing calling task with ignoring results."""
        result = add.apply_async((1, 2), ignore_result=True)
        assert result.get() is None
        # We wait since it takes a bit of time for the result to be
        # persisted in the result backend.
        sleep(1)
        assert result.result is None

    @flaky
    def test_timeout(self, manager):
        """Testing timeout of getting results from tasks."""
        result = sleeping.delay(10)
        with pytest.raises(celery.exceptions.TimeoutError):
            result.get(timeout=5)

    @flaky
    def test_expired(self, manager):
        """Testing expiration of task."""
        # Fill the queue with tasks which took > 1 sec to process
        for _ in range(4):
            sleeping.delay(2)
        # Execute task with expiration = 1 sec
        result = add.apply_async((1, 1), expires=1)
        with pytest.raises(celery.exceptions.TaskRevokedError):
            result.get()
        assert result.status == 'REVOKED'
        assert result.ready() is True
        assert result.failed() is False
        assert result.successful() is False

        # Fill the queue with tasks which took > 1 sec to process
        for _ in range(4):
            sleeping.delay(2)
        # Execute task with expiration at now + 1 sec
        result = add.apply_async((1, 1), expires=datetime.utcnow() + timedelta(seconds=1))
        with pytest.raises(celery.exceptions.TaskRevokedError):
            result.get()
        assert result.status == 'REVOKED'
        assert result.ready() is True
        assert result.failed() is False
        assert result.successful() is False

    @flaky
    def test_eta(self, manager):
        """Tests tasks scheduled at some point in future."""
        start = perf_counter()
        # Schedule task to be executed in 3 seconds
        result = add.apply_async((1, 1), countdown=3)
        sleep(1)
        assert result.status == 'PENDING'
        assert result.ready() is False
        assert result.get() == 2
        end = perf_counter()
        assert result.status == 'SUCCESS'
        assert result.ready() is True
        # Difference between calling the task and result must be bigger than 3 secs
        assert (end - start) > 3

        start = perf_counter()
        # Schedule task to be executed at time now + 3 seconds
        result = add.apply_async((2, 2), eta=datetime.utcnow() + timedelta(seconds=3))
        sleep(1)
        assert result.status == 'PENDING'
        assert result.ready() is False
        assert result.get() == 4
        end = perf_counter()
        assert result.status == 'SUCCESS'
        assert result.ready() is True
        # Difference between calling the task and result must be bigger than 3 secs
        assert (end - start) > 3

    @flaky
    def test_fail(self, manager):
        """Tests that the failing task propagates back correct exception."""
        result = fail.delay()
        with pytest.raises(ExpectedException):
            result.get(timeout=5)
        assert result.status == 'FAILURE'
        assert result.ready() is True
        assert result.failed() is True
        assert result.successful() is False

    @flaky
    def test_revoked(self, manager):
        """Testing revoking of task"""
        # Fill the queue with tasks to fill the queue
        for _ in range(4):
            sleeping.delay(2)
        # Execute task and revoke it
        result = add.apply_async((1, 1))
        result.revoke()
        with pytest.raises(celery.exceptions.TaskRevokedError):
            result.get()
        assert result.status == 'REVOKED'
        assert result.ready() is True
        assert result.failed() is False
        assert result.successful() is False

    def test_revoked_by_headers_simple_canvas(self, manager):
        """Testing revoking of task using a stamped header"""
        target_monitoring_id = uuid4().hex

        class MonitoringIdStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {'monitoring_id': target_monitoring_id}

        for monitoring_id in [target_monitoring_id, uuid4().hex, 4242, None]:
            stamped_task = add.si(1, 1)
            stamped_task.stamp(visitor=MonitoringIdStampingVisitor())
            result = stamped_task.freeze()
            result.revoke_by_stamped_headers(headers={'monitoring_id': [monitoring_id]})
            stamped_task.apply_async()
            if monitoring_id == target_monitoring_id:
                with pytest.raises(celery.exceptions.TaskRevokedError):
                    result.get()
                assert result.status == 'REVOKED'
                assert result.ready() is True
                assert result.failed() is False
                assert result.successful() is False
            else:
                assert result.get() == 2
                assert result.status == 'SUCCESS'
                assert result.ready() is True
                assert result.failed() is False
                assert result.successful() is True

            # Clear the set of revoked stamps in the worker state.
            # This step is performed in each iteration of the loop to ensure that only tasks
            # stamped with a specific monitoring ID will be revoked.
            # For subsequent iterations with different monitoring IDs, the revoked stamps will
            # not match the task's stamps, allowing those tasks to proceed successfully.
            worker_state.revoked_stamps.clear()

    def test_revoked_by_headers_complex_canvas(self, manager, subtests):
        """Testing revoking of task using a stamped header"""
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        for monitoring_id in ["4242", [1234, uuid4().hex]]:

            # Try to purge the queue before we start
            # to attempt to avoid interference from other tests
            manager.wait_until_idle()

            target_monitoring_id = isinstance(monitoring_id, list) and monitoring_id[0] or monitoring_id

            class MonitoringIdStampingVisitor(StampingVisitor):
                def on_signature(self, sig, **headers) -> dict:
                    return {'monitoring_id': target_monitoring_id, 'stamped_headers': ['monitoring_id']}

            stamped_task = sleeping.si(4)
            stamped_task.stamp(visitor=MonitoringIdStampingVisitor())
            result = stamped_task.freeze()

            canvas = [
                group([stamped_task]),
                chord(group([stamped_task]), sleeping.si(2)),
                chord(group([sleeping.si(2)]), stamped_task),
                chain(stamped_task),
                group([sleeping.si(2), stamped_task, sleeping.si(2)]),
                chord([sleeping.si(2), stamped_task], sleeping.si(2)),
                chord([sleeping.si(2), sleeping.si(2)], stamped_task),
                chain(sleeping.si(2), stamped_task),
                chain(sleeping.si(2), group([sleeping.si(2), stamped_task, sleeping.si(2)])),
                chain(sleeping.si(2), group([sleeping.si(2), stamped_task]), sleeping.si(2)),
                chain(sleeping.si(2), group([sleeping.si(2), sleeping.si(2)]), stamped_task),
            ]

            result.revoke_by_stamped_headers(headers={'monitoring_id': monitoring_id})

            for sig in canvas:
                sig_result = sig.apply_async()
                with subtests.test(msg='Testing if task was revoked'):
                    with pytest.raises(celery.exceptions.TaskRevokedError):
                        sig_result.get()
                    assert result.status == 'REVOKED'
                    assert result.ready() is True
                    assert result.failed() is False
                    assert result.successful() is False
            worker_state.revoked_stamps.clear()

    @flaky
    def test_wrong_arguments(self, manager):
        """Tests that proper exceptions are raised when task is called with wrong arguments."""
        with pytest.raises(TypeError):
            add(5)

        with pytest.raises(TypeError):
            add(5, 5, wrong_arg=5)

        with pytest.raises(TypeError):
            add.delay(5)

        with pytest.raises(TypeError):
            add.delay(5, wrong_arg=5)

        # Tasks with typing=False are not checked but execution should fail
        result = add_not_typed.delay(5)
        with pytest.raises(TypeError):
            result.get(timeout=5)
        assert result.status == 'FAILURE'

        result = add_not_typed.delay(5, wrong_arg=5)
        with pytest.raises(TypeError):
            result.get(timeout=5)
        assert result.status == 'FAILURE'

    @pytest.mark.xfail(
        condition=TEST_BACKEND == "rpc",
        reason="Retry failed on rpc backend",
        strict=False,
    )
    def test_retry(self, manager):
        """Tests retrying of task."""
        # Tests when max. retries is reached
        result = retry.delay()

        tik = time.monotonic()
        while time.monotonic() < tik + 5:
            status = result.status
            if status != 'PENDING':
                break
            sleep(0.1)
        else:
            raise AssertionError("Timeout while waiting for the task to be retried")
        assert status == 'RETRY'
        with pytest.raises(ExpectedException):
            result.get()
        assert result.status == 'FAILURE'

        # Tests when task is retried but after returns correct result
        result = retry.delay(return_value='bar')

        tik = time.monotonic()
        while time.monotonic() < tik + 5:
            status = result.status
            if status != 'PENDING':
                break
            sleep(0.1)
        else:
            raise AssertionError("Timeout while waiting for the task to be retried")
        assert status == 'RETRY'
        assert result.get() == 'bar'
        assert result.status == 'SUCCESS'

    def test_retry_with_unpickleable_exception(self, manager):
        """Test a task that retries with an unpickleable exception.

        We expect to be able to fetch the result (exception) correctly.
        """

        job = retry_unpickleable.delay(
            "foo",
            "bar",
            retry_kwargs={"countdown": 10, "max_retries": 1},
        )

        # Wait for the task to raise the Retry exception
        tik = time.monotonic()
        while time.monotonic() < tik + 5:
            status = job.status
            if status != 'PENDING':
                break
            sleep(0.1)
        else:
            raise AssertionError("Timeout while waiting for the task to be retried")

        assert status == 'RETRY'

        # Get the exception
        res = job.result
        assert job.status == 'RETRY'  # make sure that it wasn't completed yet

        # Check it
        assert isinstance(res, UnpickleableExceptionWrapper)
        assert res.exc_cls_name == "UnpickleableException"
        assert res.exc_args == ("foo",)

        job.revoke()

    def test_fail_with_unpickleable_exception(self, manager):
        """Test a task that fails with an unpickleable exception.

        We expect to be able to fetch the result (exception) correctly.
        """
        result = fail_unpickleable.delay("foo", "bar")

        with pytest.raises(UnpickleableExceptionWrapper) as exc_info:
            result.get()

        exc_wrapper = exc_info.value
        assert exc_wrapper.exc_cls_name == "UnpickleableException"
        assert exc_wrapper.exc_args == ("foo",)

        assert result.status == 'FAILURE'

    @flaky
    def test_task_accepted(self, manager, sleep=1):
        r1 = sleeping.delay(sleep)
        sleeping.delay(sleep)
        manager.assert_accepted([r1.id])

    @flaky
    def test_task_retried_once(self, manager):
        res = retry_once.delay()
        assert res.get(timeout=TIMEOUT) == 1  # retried once

    @flaky
    def test_task_retried_once_with_expires(self, manager):
        res = retry_once.delay(expires=60)
        assert res.get(timeout=TIMEOUT) == 1  # retried once

    @flaky
    def test_task_retried_priority(self, manager):
        res = retry_once_priority.apply_async(priority=7)
        assert res.get(timeout=TIMEOUT) == 7  # retried once with priority 7

    @flaky
    def test_task_retried_headers(self, manager):
        res = retry_once_headers.apply_async(headers={'x-test-header': 'test-value'})
        headers = res.get(timeout=TIMEOUT)
        assert headers is not None  # retried once with headers
        assert 'x-test-header' in headers  # retry keeps custom headers

    @flaky
    def test_unicode_task(self, manager):
        manager.join(
            group(print_unicode.s() for _ in range(5))(),
            timeout=TIMEOUT, propagate=True,
        )

    @flaky
    def test_properties(self, celery_session_worker):
        res = return_properties.apply_async(app_id="1234")
        assert res.get(timeout=TIMEOUT)["app_id"] == "1234"


class test_trace_log_arguments:
    args = "CUSTOM ARGS"
    kwargs = "CUSTOM KWARGS"

    def assert_trace_log(self, caplog, result, expected):
        # wait for logs from worker
        sleep(.01)

        records = [(r.name, r.levelno, r.msg, r.data["args"], r.data["kwargs"])
                   for r in caplog.records
                   if r.name in {'celery.worker.strategy', 'celery.app.trace'}
                   if r.data["id"] == result.task_id
                   ]
        assert records == [(*e, self.args, self.kwargs) for e in expected]

    def call_task_with_reprs(self, task):
        return task.set(argsrepr=self.args, kwargsrepr=self.kwargs).delay()

    @flaky
    def test_task_success(self, caplog):
        result = self.call_task_with_reprs(add.s(2, 2))
        value = result.get()
        assert value == 4
        assert result.successful() is True

        self.assert_trace_log(caplog, result, [
            ('celery.worker.strategy', logging.INFO,
             celery.app.trace.LOG_RECEIVED,
             ),
            ('celery.app.trace', logging.INFO,
             celery.app.trace.LOG_SUCCESS,
             ),
        ])

    @flaky
    def test_task_failed(self, caplog):
        result = self.call_task_with_reprs(fail.s(2, 2))
        with pytest.raises(ExpectedException):
            result.get(timeout=5)
        assert result.failed() is True

        self.assert_trace_log(caplog, result, [
            ('celery.worker.strategy', logging.INFO,
             celery.app.trace.LOG_RECEIVED,
             ),
            ('celery.app.trace', logging.ERROR,
             celery.app.trace.LOG_FAILURE,
             ),
        ])


class test_task_redis_result_backend:
    @pytest.fixture()
    def manager(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        return manager

    def test_ignoring_result_no_subscriptions(self, manager):
        channels_before_test = get_active_redis_channels()

        result = add_ignore_result.delay(1, 2)
        assert result.ignored is True

        new_channels = [channel for channel in get_active_redis_channels() if channel not in channels_before_test]
        assert new_channels == []

    @flaky
    def test_asyncresult_forget_cancels_subscription(self, manager):
        channels_before_test = get_active_redis_channels()

        result = add.delay(1, 2)
        assert set(get_active_redis_channels()) == {
            f"celery-task-meta-{result.id}".encode(), *channels_before_test
        }
        result.forget()

        new_channels = [channel for channel in get_active_redis_channels() if channel not in channels_before_test]
        assert new_channels == []

    def test_asyncresult_get_cancels_subscription(self, manager):
        channels_before_test = get_active_redis_channels()

        result = add.delay(1, 2)
        assert set(get_active_redis_channels()) == {
            f"celery-task-meta-{result.id}".encode(), *channels_before_test
        }
        assert result.get(timeout=3) == 3

        new_channels = [channel for channel in get_active_redis_channels() if channel not in channels_before_test]
        assert new_channels == []


class test_task_replacement:
    def test_replaced_task_nesting_level_0(self, manager):
        @task_received.connect
        def task_received_handler(request, **kwargs):
            nonlocal assertion_result

            try:
                # This tests mainly that the field even exists and set to default 0
                assertion_result = request.replaced_task_nesting < 1
            except Exception:
                assertion_result = False

        non_replaced_task = add.si(4, 2)
        res = non_replaced_task.delay()
        assertion_result = False
        assert res.get(timeout=TIMEOUT) == 6
        assert assertion_result

    def test_replaced_task_nesting_level_1(self, manager):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")

        redis_connection = get_redis_connection()
        redis_connection.delete("redis-echo")

        @task_received.connect
        def task_received_handler(request, **kwargs):
            nonlocal assertion_result

            try:
                assertion_result = request.replaced_task_nesting <= 2
            except Exception:
                assertion_result = False

        replaced_task = second_order_replace1.si()
        res = replaced_task.delay()
        assertion_result = False
        res.get(timeout=TIMEOUT)
        assert assertion_result
        redis_messages = list(redis_connection.lrange("redis-echo", 0, -1))
        expected_messages = [b"In A", b"In B", b"In/Out C", b"Out B", b"Out A"]
        assert redis_messages == expected_messages

    def test_replaced_task_nesting_chain(self, manager):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")

        redis_connection = get_redis_connection()
        redis_connection.delete("redis-echo")

        @task_received.connect
        def task_received_handler(request, **kwargs):
            nonlocal assertion_result

            try:
                assertion_result = request.replaced_task_nesting <= 3
            except Exception:
                assertion_result = False

        assertion_result = False
        chain_task = second_order_replace1.si() | add.si(4, 2)
        res = chain_task.delay()
        res.get(timeout=TIMEOUT)
        assert assertion_result
        redis_messages = list(redis_connection.lrange("redis-echo", 0, -1))
        expected_messages = [b"In A", b"In B", b"In/Out C", b"Out B", b"Out A"]
        assert redis_messages == expected_messages
