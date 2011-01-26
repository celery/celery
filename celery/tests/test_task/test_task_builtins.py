import warnings

from celery.task import ping, PingTask, backend_cleanup
from celery.tests.compat import catch_warnings
from celery.tests.utils import unittest, execute_context


def some_func(i):
    return i * i


class test_deprecated(unittest.TestCase):

    def test_ping(self):
        warnings.resetwarnings()

        def block(log):
            prev = PingTask.app.conf.CELERY_ALWAYS_EAGER
            PingTask.app.conf.CELERY_ALWAYS_EAGER = True
            try:
                return ping(), log[0].message
            finally:
                PingTask.app.conf.CELERY_ALWAYS_EAGER = prev

        pong, warning = execute_context(catch_warnings(record=True), block)
        self.assertEqual(pong, "pong")
        self.assertIsInstance(warning, DeprecationWarning)
        self.assertIn("ping task has been deprecated",
                      warning.args[0])

    def test_TaskSet_import_from_task_base(self):
        warnings.resetwarnings()

        def block(log):
            from celery.task.base import TaskSet, subtask
            TaskSet()
            subtask(PingTask)
            return log[0].message, log[1].message

        for w in execute_context(catch_warnings(record=True), block):
            self.assertIsInstance(w, DeprecationWarning)
            self.assertIn("is deprecated", w.args[0])


class test_backend_cleanup(unittest.TestCase):

    def test_run(self):
        backend_cleanup.apply()
