from __future__ import with_statement

import sys
import errno
import unittest

from django.core.exceptions import ImproperlyConfigured

from celery import states
from celery.utils import gen_unique_id
from celery.backends.amqp import AMQPBackend
from celery.datastructures import ExceptionInfo


class SomeClass(object):

    def __init__(self, data):
        self.data = data


class TestRedisBackend(unittest.TestCase):

    def setUp(self):
        self.backend = AMQPBackend()
        self.backend._use_debug_tracking = True

    def test_mark_as_done(self):
        tb = self.backend

        tid = gen_unique_id()

        tb.mark_as_done(tid, 42)
        self.assertTrue(tb.is_successful(tid))
        self.assertEquals(tb.get_status(tid), states.SUCCESS)
        self.assertEquals(tb.get_result(tid), 42)
        self.assertTrue(tb._cache.get(tid))
        self.assertTrue(tb.get_result(tid), 42)

    def test_is_pickled(self):
        tb = self.backend

        tid2 = gen_unique_id()
        result = {"foo": "baz", "bar": SomeClass(12345)}
        tb.mark_as_done(tid2, result)
        # is serialized properly.
        rindb = tb.get_result(tid2)
        self.assertEquals(rindb.get("foo"), "baz")
        self.assertEquals(rindb.get("bar").data, 12345)

    def test_mark_as_failure(self):
        tb = self.backend

        tid3 = gen_unique_id()
        try:
            raise KeyError("foo")
        except KeyError, exception:
            einfo = ExceptionInfo(sys.exc_info())
        tb.mark_as_failure(tid3, exception, traceback=einfo.traceback)
        self.assertFalse(tb.is_successful(tid3))
        self.assertEquals(tb.get_status(tid3), states.FAILURE)
        self.assertTrue(isinstance(tb.get_result(tid3), KeyError))
        self.assertEquals(tb.get_traceback(tid3), einfo.traceback)

    def test_process_cleanup(self):
        self.backend.process_cleanup()
