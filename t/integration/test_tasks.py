from datetime import datetime, timedelta
from time import perf_counter, sleep

import pytest

import celery
from celery import group

from .conftest import get_active_redis_channels
from .tasks import (ClassBasedAutoRetryTask, ExpectedException, add,
                    add_ignore_result, add_not_typed, fail, print_unicode,
                    retry, retry_once, retry_once_priority, sleeping)

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
        celery_session_app.tasks.register(task)
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

    @flaky
    def test_retry(self, manager):
        """Tests retrying of task."""
        # Tests when max. retries is reached
        result = retry.delay()
        for _ in range(5):
            status = result.status
            if status != 'PENDING':
                break
            sleep(1)
        assert status == 'RETRY'
        with pytest.raises(ExpectedException):
            result.get()
        assert result.status == 'FAILURE'

        # Tests when task is retried but after returns correct result
        result = retry.delay(return_value='bar')
        for _ in range(5):
            status = result.status
            if status != 'PENDING':
                break
            sleep(1)
        assert status == 'RETRY'
        assert result.get() == 'bar'
        assert result.status == 'SUCCESS'

    @flaky
    def test_task_accepted(self, manager, sleep=1):
        r1 = sleeping.delay(sleep)
        sleeping.delay(sleep)
        manager.assert_accepted([r1.id])

    @flaky
    def test_task_retried(self):
        res = retry_once.delay()
        assert res.get(timeout=TIMEOUT) == 1  # retried once

    @flaky
    def test_task_retried_priority(self):
        res = retry_once_priority.apply_async(priority=7)
        assert res.get(timeout=TIMEOUT) == 7  # retried once with priority 7

    @flaky
    def test_unicode_task(self, manager):
        manager.join(
            group(print_unicode.s() for _ in range(5))(),
            timeout=TIMEOUT, propagate=True,
        )


class tests_task_redis_result_backend:
    def setup(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

    def test_ignoring_result_no_subscriptions(self):
        assert get_active_redis_channels() == []
        result = add_ignore_result.delay(1, 2)
        assert result.ignored is True
        assert get_active_redis_channels() == []

    def test_asyncresult_forget_cancels_subscription(self):
        result = add.delay(1, 2)
        assert get_active_redis_channels() == [
            f"celery-task-meta-{result.id}"
        ]
        result.forget()
        assert get_active_redis_channels() == []

    def test_asyncresult_get_cancels_subscription(self):
        result = add.delay(1, 2)
        assert get_active_redis_channels() == [
            f"celery-task-meta-{result.id}"
        ]
        assert result.get(timeout=3) == 3
        assert get_active_redis_channels() == []
