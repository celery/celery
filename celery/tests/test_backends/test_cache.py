import unittest
from celery.backends.cache import CacheBackend
from celery.utils import gen_unique_id


class SomeClass(object):

    def __init__(self, data):
        self.data = data


class TestCacheBackend(unittest.TestCase):

    def test_mark_as_done(self):
        cb = CacheBackend()

        tid = gen_unique_id()

        self.assertFalse(cb.is_successful(tid))
        self.assertEquals(cb.get_status(tid), "PENDING")
        self.assertEquals(cb.get_result(tid), None)

        cb.mark_as_done(tid, 42)
        self.assertTrue(cb.is_successful(tid))
        self.assertEquals(cb.get_status(tid), "SUCCESS")
        self.assertEquals(cb.get_result(tid), 42)
        self.assertTrue(cb._cache.get(tid))
        self.assertTrue(cb.get_result(tid), 42)

    def test_is_pickled(self):
        cb = CacheBackend()

        tid2 = gen_unique_id()
        result = {"foo": "baz", "bar": SomeClass(12345)}
        cb.mark_as_done(tid2, result)
        # is serialized properly.
        rindb = cb.get_result(tid2)
        self.assertEquals(rindb.get("foo"), "baz")
        self.assertEquals(rindb.get("bar").data, 12345)

    def test_mark_as_failure(self):
        cb = CacheBackend()

        tid3 = gen_unique_id()
        try:
            raise KeyError("foo")
        except KeyError, exception:
            pass
        cb.mark_as_failure(tid3, exception)
        self.assertFalse(cb.is_successful(tid3))
        self.assertEquals(cb.get_status(tid3), "FAILURE")
        self.assertTrue(isinstance(cb.get_result(tid3), KeyError))

    def test_process_cleanup(self):
        cb = CacheBackend()

        cb.process_cleanup()
