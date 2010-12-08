from celery.tests.utils import unittest

from celery.task.builtins import PingTask, DeleteExpiredTaskMetaTask


def some_func(i):
    return i * i


class TestPingTask(unittest.TestCase):

    def test_ping(self):
        self.assertEqual(PingTask.apply().get(), 'pong')


class TestDeleteExpiredTaskMetaTask(unittest.TestCase):

    def test_run(self):
        DeleteExpiredTaskMetaTask.apply()
