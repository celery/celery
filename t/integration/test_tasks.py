from __future__ import absolute_import, unicode_literals

import pytest

from celery import group

from .conftest import flaky, get_active_redis_channels
from .tasks import add, add_ignore_result, print_unicode, retry_once, sleeping


class test_tasks:

    @flaky
    def test_task_accepted(self, manager, sleep=1):
        r1 = sleeping.delay(sleep)
        sleeping.delay(sleep)
        manager.assert_accepted([r1.id])

    @flaky
    def test_task_retried(self):
        res = retry_once.delay()
        assert res.get(timeout=10) == 1  # retried once

    @flaky
    def test_unicode_task(self, manager):
        manager.join(
            group(print_unicode.s() for _ in range(5))(),
            timeout=10, propagate=True,
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
            "celery-task-meta-{}".format(result.id)
        ]
        result.forget()
        assert get_active_redis_channels() == []

    def test_asyncresult_get_cancels_subscription(self):
        result = add.delay(1, 2)
        assert get_active_redis_channels() == [
            "celery-task-meta-{}".format(result.id)
        ]
        assert result.get(timeout=3) == 3
        assert get_active_redis_channels() == []
