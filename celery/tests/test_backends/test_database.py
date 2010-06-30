import sys
import socket
import unittest2 as unittest

from datetime import datetime

from celery.exceptions import ImproperlyConfigured

from celery import conf
from celery import states
from celery.db.models import Task, TaskSet
from celery.utils import gen_unique_id
from celery.backends.database import DatabaseBackend

from celery.tests.utils import execute_context, mask_modules


class SomeClass(object):

    def __init__(self, data):
        self.data = data


class test_DatabaseBackend(unittest.TestCase):

    def test_missing_dburi_raises_ImproperlyConfigured(self):
        prev, conf.RESULT_DBURI = conf.RESULT_DBURI, None
        try:
            self.assertRaises(ImproperlyConfigured, DatabaseBackend)
        finally:
            conf.RESULT_DBURI = prev

    def test_missing_task_id_is_PENDING(self):
        tb = DatabaseBackend()
        self.assertEqual(tb.get_status("xxx-does-not-exist"), states.PENDING)

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

    def test_save___restore_taskset(self):
        tb = DatabaseBackend()

        tid = gen_unique_id()
        res = {u"something": "special"}
        self.assertEqual(tb.save_taskset(tid, res), res)

        res2 = tb.restore_taskset(tid)
        self.assertEqual(res2, res)

        self.assertIsNone(tb.restore_taskset("xxx-nonexisting-id"))

    def test_cleanup(self):
        tb = DatabaseBackend()
        for i in range(10):
            tb.mark_as_done(gen_unique_id(), 42)
            tb.save_taskset(gen_unique_id(), {"foo": "bar"})
        s = tb.ResultSession()
        for t in s.query(Task).all():
            t.date_done = datetime.now() - tb.result_expires * 2
        for t in s.query(TaskSet).all():
            t.date_done = datetime.now() - tb.result_expires * 2
        s.commit()
        s.close()

        tb.cleanup()
        s2 = tb.ResultSession()
        self.assertEqual(s2.query(Task).count(), 0)
        self.assertEqual(s2.query(TaskSet).count(), 0)

    def test_Task__repr__(self):
        self.assertIn("foo", repr(Task("foo")))

    def test_TaskSet__repr__(self):
        self.assertIn("foo", repr(TaskSet("foo", None)))

