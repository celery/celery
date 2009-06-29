import unittest
from celery.task.builtins import PingTask, ExecuteRemoteTask
import pickle


def some_func(i):
    return i * i


class TestPingTask(unittest.TestCase):

    def test_ping(self):
        self.assertEquals(PingTask.apply().get(), 'pong')


class TestRemoteExecuteTask(unittest.TestCase):

    def test_execute_remote(self):
        self.assertEquals(ExecuteRemoteTask.apply(
                            args=[pickle.dumps(some_func), [10], {}]).get(),
                          100)
