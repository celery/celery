from __future__ import absolute_import
from __future__ import with_statement

from celery.task import ping, PingTask, backend_cleanup
from celery.exceptions import CDeprecationWarning
from celery.tests.utils import Case


def some_func(i):
    return i * i


class test_deprecated(Case):

    def test_ping(self):
        with self.assertWarnsRegex(CDeprecationWarning,
                r'ping task has been deprecated'):
            prev = PingTask.app.conf.CELERY_ALWAYS_EAGER
            PingTask.app.conf.CELERY_ALWAYS_EAGER = True
            try:
                self.assertEqual(ping(), "pong")
            finally:
                PingTask.app.conf.CELERY_ALWAYS_EAGER = prev

    def test_TaskSet_import_from_task_base(self):
        with self.assertWarnsRegex(CDeprecationWarning, r'is deprecated'):
            from celery.task.base import TaskSet, subtask
            TaskSet()
            subtask(PingTask)


class test_backend_cleanup(Case):

    def test_run(self):
        backend_cleanup.apply()
