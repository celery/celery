import pytest

from celery import group

from .conftest import get_active_redis_channels
from .tasks import (ClassBasedAutoRetryTask, add, add_ignore_result,
                    print_unicode, retry_once, retry_once_priority, sleeping)

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


class test_tasks:

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
