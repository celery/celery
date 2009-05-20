import unittest
from celery.backends.database import Backend
import uuid


class SomeClass(object):

    def __init__(self, data):
        self.data = data


class TestDatabaseBackend(unittest.TestCase):

    def test_backend(self):
        b = Backend()
        tid = str(uuid.uuid4())

        self.assertFalse(b.is_done(tid))
        self.assertEquals(b.get_status(tid), "PENDING")
        self.assertEquals(b.get_result(tid), '')

        b.mark_as_done(tid, 42)
        self.assertTrue(b.is_done(tid))
        self.assertEquals(b.get_status(tid), "DONE")
        self.assertEquals(b.get_result(tid), 42)
        self.assertTrue(b._cache.get(tid))
        self.assertTrue(b.get_result(tid), 42)

        tid2 = str(uuid.uuid4())
        result = {"foo": "baz", "bar": SomeClass(12345)}
        b.mark_as_done(tid2, result)
        # is serialized properly.
        rindb = b.get_result(tid2)
        self.assertEquals(rindb.get("foo"), "baz")
        self.assertEquals(rindb.get("bar").data, 12345)

        tid3 = str(uuid.uuid4())
        try:
            raise KeyError("foo")
        except KeyError, exception:
            pass
        b.mark_as_failure(tid3, exception)
        self.assertFalse(b.is_done(tid3))
        self.assertEquals(b.get_status(tid3), "FAILURE")
        self.assertTrue(isinstance(b.get_result(tid3), KeyError))
