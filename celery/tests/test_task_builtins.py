from celery.tests.utils import unittest

from celery.task import PingTask, backend_cleanup


def some_func(i):
    return i * i


class test_PingTask(unittest.TestCase):

    def test_ping(self):
        self.assertEqual(PingTask.apply().get(), 'pong')


class test_backend_cleanup(unittest.TestCase):

    def test_run(self):
        backend_cleanup.apply()
