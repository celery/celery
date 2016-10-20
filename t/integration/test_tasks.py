from __future__ import absolute_import, unicode_literals
from celery import group
from .tasks import print_unicode, sleeping


class test_tasks:

    def test_task_accepted(self, manager, sleep=1):
        r1 = sleeping.delay(sleep)
        sleeping.delay(sleep)
        manager.assert_accepted([r1.id])

    def test_unicode_task(self, manager):
        manager.join(
            group(print_unicode.s() for _ in range(5))(),
            timeout=10, propagate=True,
        )
