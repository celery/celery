from __future__ import absolute_import
from __future__ import with_statement

import warnings

from celery.task import ping, PingTask, backend_cleanup
from celery.exceptions import CDeprecationWarning
from celery.tests.compat import catch_warnings
from celery.tests.utils import unittest


def some_func(i):
    return i * i


class test_deprecated(unittest.TestCase):

    def test_ping(self):
        warnings.resetwarnings()

        with catch_warnings(record=True) as log:
            prev = PingTask.app.conf.CELERY_ALWAYS_EAGER
            PingTask.app.conf.CELERY_ALWAYS_EAGER = True
            try:
                pong = ping()
                warning = log[0].message
                self.assertEqual(pong, "pong")
                self.assertIsInstance(warning, CDeprecationWarning)
                self.assertIn("ping task has been deprecated",
                              warning.args[0])
            finally:
                PingTask.app.conf.CELERY_ALWAYS_EAGER = prev

    def test_TaskSet_import_from_task_base(self):
        warnings.resetwarnings()

        with catch_warnings(record=True) as log:
            from celery.task.base import TaskSet, subtask
            TaskSet()
            subtask(PingTask)
            for w in (log[0].message, log[1].message):
                self.assertIsInstance(w, CDeprecationWarning)
                self.assertIn("is deprecated", w.args[0])


class test_backend_cleanup(unittest.TestCase):

    def test_run(self):
        backend_cleanup.apply()
