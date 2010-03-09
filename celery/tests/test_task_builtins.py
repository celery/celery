import unittest

from billiard.serialization import pickle

from celery.task.base import ExecuteRemoteTask
from celery.task.builtins import PingTask, DeleteExpiredTaskMetaTask


def some_func(i):
    return i * i


class TestPingTask(unittest.TestCase):

    def test_ping(self):
        self.assertEqual(PingTask.apply().get(), 'pong')


class TestRemoteExecuteTask(unittest.TestCase):

    def test_execute_remote(self):
        self.assertEqual(ExecuteRemoteTask.apply(
                            args=[pickle.dumps(some_func), [10], {}]).get(),
                          100)


class TestDeleteExpiredTaskMetaTask(unittest.TestCase):

    def test_run(self):
        DeleteExpiredTaskMetaTask.apply()
