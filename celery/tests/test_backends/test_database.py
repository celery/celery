import unittest2 as unittest
from datetime import timedelta

from celery import states
from celery.task import PeriodicTask
from celery.utils import gen_unique_id
from celery.backends.database import DatabaseBackend


class SomeClass(object):

    def __init__(self, data):
        self.data = data


class MyPeriodicTask(PeriodicTask):
    name = "c.u.my-periodic-task-244"
    run_every = timedelta(seconds=1)

    def run(self, **kwargs):
        return 42


class TestDatabaseBackend(unittest.TestCase):

    def test_backend(self):
        b = DatabaseBackend()
        tid = gen_unique_id()

        self.assertFalse(b.is_successful(tid))
        self.assertEqual(b.get_status(tid), states.PENDING)
        self.assertIsNone(b.get_result(tid))

        b.mark_as_done(tid, 42)
        self.assertTrue(b.is_successful(tid))
        self.assertEqual(b.get_status(tid), states.SUCCESS)
        self.assertEqual(b.get_result(tid), 42)

        tid2 = gen_unique_id()
        result = {"foo": "baz", "bar": SomeClass(12345)}
        b.mark_as_done(tid2, result)
        # is serialized properly.
        rindb = b.get_result(tid2)
        self.assertEqual(rindb.get("foo"), "baz")
        self.assertEqual(rindb.get("bar").data, 12345)

        tid3 = gen_unique_id()
        try:
            raise KeyError("foo")
        except KeyError, exception:
            pass
        b.mark_as_failure(tid3, exception)
        self.assertFalse(b.is_successful(tid3))
        self.assertEqual(b.get_status(tid3), states.FAILURE)
        self.assertIsInstance(b.get_result(tid3), KeyError)

    def test_taskset_store(self):
        b = DatabaseBackend()
        tid = gen_unique_id()

        self.assertIsNone(b.restore_taskset(tid))

        result = {"foo": "baz", "bar": SomeClass(12345)}
        b.save_taskset(tid, result)
        rindb = b.restore_taskset(tid)
        self.assertIsNotNone(rindb)
        self.assertEqual(rindb.get("foo"), "baz")
        self.assertEqual(rindb.get("bar").data, 12345)
