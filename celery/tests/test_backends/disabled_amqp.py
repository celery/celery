import sys
import unittest2 as unittest

from celery import states
from celery.utils import gen_unique_id
from celery.backends.amqp import AMQPBackend
from celery.datastructures import ExceptionInfo


class SomeClass(object):

    def __init__(self, data):
        self.data = data


class test_AMQPBackend(unittest.TestCase):

    def create_backend(self):
        return AMQPBackend(serializer="pickle", persistent=False)

    def test_mark_as_done(self):
        tb1 = self.create_backend()
        tb2 = self.create_backend()

        tid = gen_unique_id()

        tb1.mark_as_done(tid, 42)
        self.assertTrue(tb2.is_successful(tid))
        self.assertEqual(tb2.get_status(tid), states.SUCCESS)
        self.assertEqual(tb2.get_result(tid), 42)
        self.assertTrue(tb2._cache.get(tid))
        self.assertTrue(tb2.get_result(tid), 42)

    def test_is_pickled(self):
        tb1 = self.create_backend()
        tb2 = self.create_backend()

        tid2 = gen_unique_id()
        result = {"foo": "baz", "bar": SomeClass(12345)}
        tb1.mark_as_done(tid2, result)
        # is serialized properly.
        rindb = tb2.get_result(tid2)
        self.assertEqual(rindb.get("foo"), "baz")
        self.assertEqual(rindb.get("bar").data, 12345)

    def test_mark_as_failure(self):
        tb1 = self.create_backend()
        tb2 = self.create_backend()

        tid3 = gen_unique_id()
        try:
            raise KeyError("foo")
        except KeyError, exception:
            einfo = ExceptionInfo(sys.exc_info())
        tb1.mark_as_failure(tid3, exception, traceback=einfo.traceback)
        self.assertFalse(tb2.is_successful(tid3))
        self.assertEqual(tb2.get_status(tid3), states.FAILURE)
        self.assertIsInstance(tb2.get_result(tid3), KeyError)
        self.assertEqual(tb2.get_traceback(tid3), einfo.traceback)

    def test_process_cleanup(self):
        self.create_backend().process_cleanup()
