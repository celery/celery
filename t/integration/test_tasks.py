import logging
import platform
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import billiard as multiprocessing
import pytest

import celery
from celery import chain, chord, group
from celery.canvas import StampingVisitor
from celery.signals import task_received
from celery.utils.serialization import UnpickleableExceptionWrapper
from celery.worker import state as worker_state

from .conftest import TEST_BACKEND, get_active_redis_channels, get_redis_connection
from .tasks import (ClassBasedAutoRetryTask, ExpectedException, add, add_ignore_result, add_not_typed, add_pydantic,
                    add_pydantic_string_annotations, fail, fail_unpickleable, print_unicode, retry, retry_once,
                    retry_once_headers, retry_once_priority, retry_unpickleable, return_properties,
                    return_request_time_limits, second_order_replace1, sleeping,
                    soft_time_limit_must_exceed_time_limit, task_with_declared_time_limits)

TIMEOUT = 10

_flaky = pytest.mark.flaky(reruns=2, reruns_delay=1)
_timeout = pytest.mark.timeout(timeout=120)


def flaky(fn):
    return _timeout(_flaky(fn))


def set_multiprocessing_start_method():
    """Set multiprocessing start method to 'fork' if not on Linux."""
    if platform.system() != "Linux":
        try:
            multiprocessing.set_start_method("fork")
        except RuntimeError:
            pass


def _target_monitoring_id(monitoring_id):
    return monitoring_id[0] if isinstance(monitoring_id, list) else monitoring_id


def _clear_revoked_stamps():
    worker_state.revoked_stamps.clear()


def _make_monitoring_visitor(target_monitoring_id):
    class MonitoringIdStampingVisitor(StampingVisitor):
        def on_signature(self, sig, **headers) -> dict:
            return {
                'monitoring_id': target_monitoring_id,
                'stamped_headers': ['monitoring_id'],
            }
    return MonitoringIdStampingVisitor()


def _make_stamped_sleeping_task(target_monitoring_id, delay=1):
    stamped_task = sleeping.si(delay)
    stamped_task.stamp(visitor=_make_monitoring_visitor(target_monitoring_id))
    result = stamped_task.freeze()
    return stamped_task, result


CANVAS_BUILDERS = {
    "group_single": lambda stamped: group([stamped]),
    "chord_header_revoked": lambda stamped: chord(group([stamped]), sleeping.si(1)),
    "chord_body_revoked": lambda stamped: chord(group([sleeping.si(1)]), stamped),
    "chain_single": lambda stamped: chain(stamped),
    "group_mixed": lambda stamped: group([sleeping.si(1), stamped, sleeping.si(1)]),
    "chord_mixed_header": lambda stamped: chord([sleeping.si(1), stamped], sleeping.si(1)),
    "chord_revoked_body": lambda stamped: chord([sleeping.si(1), sleeping.si(1)], stamped),
    "chain_second": lambda stamped: chain(sleeping.si(1), stamped),
    "chain_group_mixed": lambda stamped: chain(
        sleeping.si(1),
        group([sleeping.si(1), stamped, sleeping.si(1)]),
    ),
    "chain_group_then_tail": lambda stamped: chain(
        sleeping.si(1),
        group([sleeping.si(1), stamped]),
        sleeping.si(1),
    ),
    "chain_group_to_revoked_body": lambda stamped: chain(
        sleeping.si(1),
        group([sleeping.si(1), sleeping.si(1)]),
        stamped,
    ),
}


class test_class_based_tasks:

    @flaky
    def test_class_based_task_retried(self, celery_session_app, celery_session_worker):
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
        for i in range(10):
            results.append([i + i, add.delay(i, i)])
        for expected, result in results:
            value = result.get(timeout=10)
            assert value == expected
            assert result.status == 'SUCCESS'
            assert result.ready() is True
            assert result.successful() is True

        results = []
        for i in range(10):
            results.append([3 * i, add.delay(i, i, z=i)])
        for expected, result in results:
            value = result.get(timeout=10)
            assert value == expected
            assert result.status == 'SUCCESS'
            assert result.ready() is True
            assert result.successful() is True

    @flaky
    @pytest.mark.skip(reason="Broken test")
    def test_multiprocess_producer(self, manager):
        """Testing multiple processes calling tasks."""
        set_multiprocessing_start_method()

        from multiprocessing import Pool
        pool = Pool(20)
        ret = pool.map(_producer, range(120))
        assert list(ret) == list(range(120))

    @flaky
    @pytest.mark.skip(reason="Broken test")
    def test_multithread_producer(self, manager):
        """Testing multiple threads calling tasks."""
        set_multiprocessing_start_method()

        from multiprocessing.pool import ThreadPool
        pool = ThreadPool(20)
        ret = pool.map(_producer, range(120))
        assert list(ret) == list(range(120))

    @flaky
    def test_ignore_result(self, manager):
        """Testing calling task with ignoring results."""
        result = add.apply_async((1, 2), ignore_result=True)
        assert result.get(timeout=3) is None
        time.sleep(0.2)
        assert result.result is None

    @flaky
    def test_pydantic_annotations(self, manager):
        """Tests task call with Pydantic model serialization."""
        results = []
        for i in range(10):
            results.append([i + i, add_pydantic.delay({'x': i, 'y': i})])
        for expected, result in results:
            value = result.get(timeout=10)
            assert value == {'result': expected}
            assert result.status == 'SUCCESS'
            assert result.ready() is True
            assert result.successful() is True

    @flaky
    def test_pydantic_string_annotations(self, manager):
        """Tests task call with string-annotated Pydantic model."""
        results = []
        for i in range(10):
            results.append([i + i, add_pydantic_string_annotations.delay({'x': i, 'y': i})])
        for expected, result in results:
            value = result.get(timeout=10)
            assert value == {'result': expected}
            assert result.status == 'SUCCESS'
            assert result.ready() is True
            assert result.successful() is True

    @flaky
    def test_timeout(self, manager):
        """Testing timeout of getting results from tasks."""
        result = sleeping.delay(2)
        with pytest.raises(celery.exceptions.TimeoutError):
            result.get(timeout=0.5)

    @pytest.mark.timeout(30)
    @flaky
    def test_expired(self, manager):
        """Testing expiration of task."""
        for _ in range(2):
            sleeping.delay(1)
        result = add.apply_async((1, 1), expires=0.2)
        with pytest.raises(celery.exceptions.TaskRevokedError):
            result.get(timeout=5)
        assert result.status == 'REVOKED'
        assert result.ready() is True
        assert result.failed() is False
        assert result.successful() is False

        for _ in range(2):
            sleeping.delay(1)
        result = add.apply_async((1, 1), expires=datetime.now(timezone.utc) + timedelta(seconds=0.2))
        with pytest.raises(celery.exceptions.TaskRevokedError):
            result.get(timeout=5)
        assert result.status == 'REVOKED'
        assert result.ready() is True
        assert result.failed() is False
        assert result.successful() is False

    @flaky
    def test_eta(self, manager):
        """Tests tasks scheduled at some point in future."""
        start = time.perf_counter()
        result = add.apply_async((1, 1), countdown=1)
        time.sleep(0.2)
        assert result.status == 'PENDING'
        assert result.ready() is False
        assert result.get(timeout=5) == 2
        end = time.perf_counter()
        assert result.status == 'SUCCESS'
        assert result.ready() is True
        assert (end - start) > 1

        start = time.perf_counter()
        result = add.apply_async((2, 2), eta=datetime.now(timezone.utc) + timedelta(seconds=1))
        time.sleep(0.2)
        assert result.status == 'PENDING'
        assert result.ready() is False
        assert result.get(timeout=5) == 4
        end = time.perf_counter()
        assert result.status == 'SUCCESS'
        assert result.ready() is True
        assert (end - start) > 1

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
        for _ in range(2):
            sleeping.delay(1)
        result = add.apply_async((1, 1))
        result.revoke()
        with pytest.raises(celery.exceptions.TaskRevokedError):
            result.get(timeout=5)
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
                    result.get(timeout=5)
                assert result.status == 'REVOKED'
                assert result.ready() is True
                assert result.failed() is False
                assert result.successful() is False
            else:
                assert result.get(timeout=5) == 2
                assert result.status == 'SUCCESS'
                assert result.ready() is True
                assert result.failed() is False
                assert result.successful() is True
            _clear_revoked_stamps()

    @pytest.mark.parametrize(
        "monitoring_id",
        [
            pytest.param("4242", id="scalar-monitoring-id"),
            pytest.param([1234, "unused-id"], id="list-monitoring-id"),
        ],
    )
    @pytest.mark.parametrize("canvas_name", list(CANVAS_BUILDERS))
    @pytest.mark.timeout(20)
    def test_revoked_by_headers_complex_canvas(self, manager, monitoring_id, canvas_name):
        """Test revoke-by-stamped-headers across complex canvas shapes."""
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        manager.wait_until_idle()
        target_monitoring_id = _target_monitoring_id(monitoring_id)
        stamped_task, result = _make_stamped_sleeping_task(target_monitoring_id)

        result.revoke_by_stamped_headers(headers={'monitoring_id': monitoring_id})
        sig = CANVAS_BUILDERS[canvas_name](stamped_task)

        try:
            sig_result = sig.apply_async()
            with pytest.raises((
                celery.exceptions.TaskRevokedError,
                celery.exceptions.ChordError,
            )):
                sig_result.get(timeout=8)

            assert result.status == 'REVOKED'
            assert result.ready() is True
            assert result.failed() is False
            assert result.successful() is False
        finally:
            _clear_revoked_stamps()
            manager.wait_until_idle()

    @flaky
    def test_revoke_by_stamped_headers_no_match(self, manager):
        response = manager.app.control.revoke_by_stamped_headers(
            {"myheader": ["myvalue"]},
            terminate=False,
            reply=True,
        )

        expected_response = "headers {'myheader': ['myvalue']} flagged as revoked, but not terminated"
        assert response[0][list(response[0].keys())[0]]["ok"] == expected_response

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
        result = retry.delay()

        tik = time.monotonic()
        while time.monotonic() < tik + 5:
            status = result.status
            if status != 'PENDING':
                break
            time.sleep(0.1)
        else:
            raise AssertionError("Timeout while waiting for the task to be retried")
        assert status == 'RETRY'
        with pytest.raises(ExpectedException):
            result.get(timeout=5)
        assert result.status == 'FAILURE'

        result = retry.delay(return_value='bar')

        tik = time.monotonic()
        while time.monotonic() < tik + 5:
            status = result.status
            if status != 'PENDING':
                break
            time.sleep(0.1)
        else:
            raise AssertionError("Timeout while waiting for the task to be retried")
        assert status == 'RETRY'
        assert result.get(timeout=10) == 'bar'
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

        tik = time.monotonic()
        while time.monotonic() < tik + 5:
            status = job.status
            if status != 'PENDING':
                break
            time.sleep(0.1)
        else:
            raise AssertionError("Timeout while waiting for the task to be retried")

        assert status == 'RETRY'

        res = job.result
        assert job.status == 'RETRY'

        if isinstance(res, UnpickleableExceptionWrapper):
            assert res.exc_cls_name == "UnpickleableException"
            assert res.exc_args == ("foo",)
        else:
            res_str = str(res)
            assert "UnpickleableException" in res_str
            assert "foo" in res_str

        job.revoke()

    def test_fail_with_unpickleable_exception(self, manager):
        """Test a task that fails with an unpickleable exception.

        We expect to be able to fetch the result (exception) correctly.
        """
        result = fail_unpickleable.delay("foo", "bar")

        try:
            result.get(timeout=5)
            pytest.fail("Expected an exception when getting result")
        except UnpickleableExceptionWrapper as exc_wrapper:
            assert exc_wrapper.exc_cls_name == "UnpickleableException"
            assert exc_wrapper.exc_args == ("foo",)
        except Exception as exc:
            exc_str = str(exc)
            assert "UnpickleableException" in exc_str
            assert "foo" in exc_str

        assert result.status == 'FAILURE'

    @pytest.mark.skip(reason="Randomly fails")
    def test_task_accepted(self, manager, sleep=1):
        r1 = sleeping.delay(sleep)
        sleeping.delay(sleep)
        manager.assert_accepted([r1.id])

    @flaky
    def test_task_retried_once(self, manager):
        res = retry_once.delay()
        assert res.get(timeout=TIMEOUT) == 1

    @flaky
    def test_task_retried_once_with_expires(self, manager):
        res = retry_once.delay(expires=60)
        assert res.get(timeout=TIMEOUT) == 1

    @flaky
    def test_task_retried_priority(self, manager):
        res = retry_once_priority.apply_async(priority=7)
        assert res.get(timeout=TIMEOUT) == 7

    @flaky
    def test_task_retried_headers(self, manager):
        res = retry_once_headers.apply_async(headers={'x-test-header': 'test-value'})
        headers = res.get(timeout=TIMEOUT)
        assert headers is not None
        assert 'x-test-header' in headers

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

    @flaky
    def test_soft_time_limit_exceeding_time_limit(self):
        with pytest.raises(ValueError, match='soft_time_limit must be less than or equal to time_limit'):
            result = soft_time_limit_must_exceed_time_limit.apply_async()
            result.get(timeout=5)

            assert result.status == 'FAILURE'

    @flaky
    def test_request_time_limits_set_via_apply_async(self, manager):
        result = return_request_time_limits.apply_async(time_limit=30, soft_time_limit=20)
        data = result.get(timeout=TIMEOUT)
        assert data['time_limit'] == 30
        assert data['soft_time_limit'] == 20

    @flaky
    def test_request_time_limits_none_when_not_configured(self, manager):
        result = return_request_time_limits.apply_async()
        data = result.get(timeout=TIMEOUT)
        assert data['time_limit'] is None
        assert data['soft_time_limit'] is None

    @flaky
    def test_request_time_limits_from_task_declaration(self, manager):
        result = task_with_declared_time_limits.apply_async()
        data = result.get(timeout=TIMEOUT)
        assert data['time_limit'] == 60
        assert data['soft_time_limit'] == 45

    @flaky
    def test_apply_async_time_limits_override_task_declaration(self, manager):
        result = task_with_declared_time_limits.apply_async(time_limit=10, soft_time_limit=5)
        data = result.get(timeout=TIMEOUT)
        assert data['time_limit'] == 10
        assert data['soft_time_limit'] == 5


class test_apply_tasks:
    """Tests for tasks called via apply() method."""

    def test_apply_single_task_ids(self, manager):
        @manager.app.task(bind=True)
        def single_apply_task(self):
            return {
                'task_id': self.request.id,
                'parent_id': self.request.parent_id,
                'root_id': self.request.root_id,
            }

        result = single_apply_task.apply()
        data = result.get()

        assert data['parent_id'] is None
        assert data['root_id'] == data['task_id']

    def test_apply_nested_parent_child_relationship(self, manager):

        @manager.app.task(bind=True)
        def grandchild_task(task_self):
            return {
                'task_id': task_self.request.id,
                'parent_id': task_self.request.parent_id,
                'root_id': task_self.request.root_id,
                'name': 'grandchild_task'
            }

        @manager.app.task(bind=True)
        def child_task(task_self):
            grandchild_data = grandchild_task.apply().get()
            return {
                'task_id': task_self.request.id,
                'parent_id': task_self.request.parent_id,
                'root_id': task_self.request.root_id,
                'name': 'child_task',
                'grandchild_data': grandchild_data
            }

        @manager.app.task(bind=True)
        def parent_task(task_self):
            child_data = child_task.apply().get()
            parent_data = {
                'task_id': task_self.request.id,
                'parent_id': task_self.request.parent_id,
                'root_id': task_self.request.root_id,
                'name': 'parent_task',
                'child_data': child_data
            }
            return parent_data

        result = parent_task.apply()

        parent_data = result.get()
        child_data = parent_data['child_data']
        grandchild_data = child_data['grandchild_data']

        assert parent_data['name'] == 'parent_task'
        assert parent_data['parent_id'] is None
        assert parent_data['root_id'] == parent_data['task_id']

        assert child_data['name'] == 'child_task'
        assert child_data['parent_id'] == parent_data['task_id']
        assert child_data['root_id'] == parent_data['task_id']

        assert grandchild_data['name'] == 'grandchild_task'
        assert grandchild_data['parent_id'] == child_data['task_id']
        assert grandchild_data['root_id'] == parent_data['task_id']


class test_trace_log_arguments:
    args = "CUSTOM ARGS"
    kwargs = "CUSTOM KWARGS"

    def assert_trace_log(self, caplog, result, expected):
        time.sleep(.01)

        records = [
            (r.name, r.levelno, r.msg, r.data["args"], r.data["kwargs"])
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
        value = result.get(timeout=5)
        assert value == 4
        assert result.successful() is True

        self.assert_trace_log(caplog, result, [
            ('celery.worker.strategy', logging.INFO, celery.app.trace.LOG_RECEIVED),
            ('celery.app.trace', logging.INFO, celery.app.trace.LOG_SUCCESS),
        ])

    @flaky
    def test_task_failed(self, caplog):
        result = self.call_task_with_reprs(fail.s(2, 2))
        with pytest.raises(ExpectedException):
            result.get(timeout=5)
        assert result.failed() is True

        self.assert_trace_log(caplog, result, [
            ('celery.worker.strategy', logging.INFO, celery.app.trace.LOG_RECEIVED),
            ('celery.app.trace', logging.ERROR, celery.app.trace.LOG_FAILURE),
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

    @flaky
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


class test_pool_acquire_timeout:
    """Integration tests for broker_pool_acquire_timeout setting (#9929)."""

    @flaky
    def test_task_succeeds_with_pool_timeout_configured(self, manager):
        app = manager.app
        orig = app.conf.broker_pool_acquire_timeout
        app.conf.broker_pool_acquire_timeout = 30
        try:
            result = add.delay(1, 2)
            assert result.get(timeout=TIMEOUT) == 3
        finally:
            app.conf.broker_pool_acquire_timeout = orig

    @flaky
    def test_pool_timeout_none_blocks_successfully(self, manager):
        app = manager.app
        assert app.conf.broker_pool_acquire_timeout is None
        result = add.delay(4, 5)
        assert result.get(timeout=TIMEOUT) == 9

    @flaky
    def test_concurrent_apply_async_with_timeout(self, manager):
        from concurrent.futures import ThreadPoolExecutor, as_completed

        app = manager.app
        orig_timeout = app.conf.broker_pool_acquire_timeout
        app.conf.broker_pool_acquire_timeout = 10
        try:
            results = []
            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = [
                    executor.submit(add.delay, i, i)
                    for i in range(50)
                ]
                for future in as_completed(futures):
                    results.append(future.result())
            for r in results:
                assert r.get(timeout=TIMEOUT) is not None
        finally:
            app.conf.broker_pool_acquire_timeout = orig_timeout
