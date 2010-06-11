import sys
import socket
import unittest2 as unittest

from celery.exceptions import ImproperlyConfigured

from celery import states
from celery.utils import gen_unique_id
from celery.backends.database import DatabaseBackend

from celery.tests.utils import execute_context, mask_modules


class SomeClass(object):

    def __init__(self, data):
        self.data = data


class test_DatabaseBackend(unittest.TestCase):

    def test_mark_as_done(self):
        tb = DatabaseBackend()

        tid = gen_unique_id()

        self.assertEqual(tb.get_status(tid), states.PENDING)
        self.assertIsNone(tb.get_result(tid))

        tb.mark_as_done(tid, 42)
        self.assertEqual(tb.get_status(tid), states.SUCCESS)
        self.assertEqual(tb.get_result(tid), 42)

    def test_is_pickled(self):
        tb = DatabaseBackend()

        tid2 = gen_unique_id()
        result = {"foo": "baz", "bar": SomeClass(12345)}
        tb.mark_as_done(tid2, result)
        # is serialized properly.
        rindb = tb.get_result(tid2)
        self.assertEqual(rindb.get("foo"), "baz")
        self.assertEqual(rindb.get("bar").data, 12345)

    def test_mark_as_started(self):
        tb = DatabaseBackend()
        tid = gen_unique_id()
        tb.mark_as_started(tid)
        self.assertEqual(tb.get_status(tid), states.STARTED)

    def test_mark_as_revoked(self):
        tb = DatabaseBackend()
        tid = gen_unique_id()
        tb.mark_as_revoked(tid)
        self.assertEqual(tb.get_status(tid), states.REVOKED)

    def test_mark_as_retry(self):
        tb = DatabaseBackend()
        tid = gen_unique_id()
        try:
            raise KeyError("foo")
        except KeyError, exception:
            import traceback
            trace = "\n".join(traceback.format_stack())
        tb.mark_as_retry(tid, exception, traceback=trace)
        self.assertEqual(tb.get_status(tid), states.RETRY)
        self.assertIsInstance(tb.get_result(tid), KeyError)
        self.assertEqual(tb.get_traceback(tid), trace)

    def test_mark_as_failure(self):
        tb = DatabaseBackend()

        tid3 = gen_unique_id()
        try:
            raise KeyError("foo")
        except KeyError, exception:
            import traceback
            trace = "\n".join(traceback.format_stack())
        tb.mark_as_failure(tid3, exception, traceback=trace)
        self.assertEqual(tb.get_status(tid3), states.FAILURE)
        self.assertIsInstance(tb.get_result(tid3), KeyError)
        self.assertEqual(tb.get_traceback(tid3), trace)

    def test_process_cleanup(self):
        tb = DatabaseBackend()
        tb.process_cleanup()
