import unittest
from celery.backends.database import DatabaseBackend
from celery.utils import gen_unique_id
from celery.task import PeriodicTask
from celery import registry
from datetime import timedelta


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
        self.assertEquals(b.get_status(tid), "PENDING")
        self.assertTrue(b.get_result(tid) is None)

        b.mark_as_done(tid, 42)
        self.assertTrue(b.is_successful(tid))
        self.assertEquals(b.get_status(tid), "SUCCESS")
        self.assertEquals(b.get_result(tid), 42)
        self.assertTrue(b._cache.get(tid))
        self.assertTrue(b.get_result(tid), 42)

        tid2 = gen_unique_id()
        result = {"foo": "baz", "bar": SomeClass(12345)}
        b.mark_as_done(tid2, result)
        # is serialized properly.
        rindb = b.get_result(tid2)
        self.assertEquals(rindb.get("foo"), "baz")
        self.assertEquals(rindb.get("bar").data, 12345)

        tid3 = gen_unique_id()
        try:
            raise KeyError("foo")
        except KeyError, exception:
            pass
        b.mark_as_failure(tid3, exception)
        self.assertFalse(b.is_successful(tid3))
        self.assertEquals(b.get_status(tid3), "FAILURE")
        self.assertTrue(isinstance(b.get_result(tid3), KeyError))
