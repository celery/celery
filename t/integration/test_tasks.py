from __future__ import absolute_import, unicode_literals

from celery import group

from .conftest import flaky
from .tasks import print_unicode, retry_once, sleeping


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
