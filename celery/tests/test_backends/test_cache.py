import sys
import types
from celery.tests.utils import unittest

from celery import states
from celery.backends.cache import CacheBackend, DummyClient
from celery.exceptions import ImproperlyConfigured
from celery.result import AsyncResult
from celery.utils import gen_unique_id

from celery.tests.utils import mask_modules


class SomeClass(object):

    def __init__(self, data):
        self.data = data


class test_CacheBackend(unittest.TestCase):

    def test_mark_as_done(self):
        tb = CacheBackend(backend="memory://")

        tid = gen_unique_id()

        self.assertEqual(tb.get_status(tid), states.PENDING)
        self.assertIsNone(tb.get_result(tid))

        tb.mark_as_done(tid, 42)
        self.assertEqual(tb.get_status(tid), states.SUCCESS)
        self.assertEqual(tb.get_result(tid), 42)

    def test_is_pickled(self):
        tb = CacheBackend(backend="memory://")

        tid2 = gen_unique_id()
        result = {"foo": "baz", "bar": SomeClass(12345)}
        tb.mark_as_done(tid2, result)
        # is serialized properly.
        rindb = tb.get_result(tid2)
        self.assertEqual(rindb.get("foo"), "baz")
        self.assertEqual(rindb.get("bar").data, 12345)

    def test_mark_as_failure(self):
        tb = CacheBackend(backend="memory://")

        tid3 = gen_unique_id()
        try:
            raise KeyError("foo")
        except KeyError, exception:
            pass
        tb.mark_as_failure(tid3, exception)
        self.assertEqual(tb.get_status(tid3), states.FAILURE)
        self.assertIsInstance(tb.get_result(tid3), KeyError)

    def test_forget(self):
        tb = CacheBackend(backend="memory://")
        tid = gen_unique_id()
        tb.mark_as_done(tid, {"foo": "bar"})
        x = AsyncResult(tid, backend=tb)
        x.forget()
        self.assertIsNone(x.result)

    def test_process_cleanup(self):
        tb = CacheBackend(backend="memory://")
        tb.process_cleanup()

    def test_expires_as_int(self):
        tb = CacheBackend(backend="memory://", expires=10)
        self.assertEqual(tb.expires, 10)

    def test_unknown_backend_raises_ImproperlyConfigured(self):
        self.assertRaises(ImproperlyConfigured,
                          CacheBackend, backend="unknown://")


class MyClient(DummyClient):
    pass


class test_get_best_memcache(unittest.TestCase):

    def mock_memcache(self):
        memcache = types.ModuleType("memcache")
        memcache.Client = MyClient
        memcache.Client.__module__ = memcache.__name__
        prev, sys.modules["memcache"] = sys.modules.get("memcache"), memcache
        yield True
        if prev is not None:
            sys.modules["memcache"] = prev
        yield True

    def mock_pylibmc(self):
        pylibmc = types.ModuleType("pylibmc")
        pylibmc.Client = MyClient
        pylibmc.Client.__module__ = pylibmc.__name__
        prev = sys.modules.get("pylibmc")
        sys.modules["pylibmc"] = pylibmc
        yield True
        if prev is not None:
            sys.modules["pylibmc"] = prev
        yield True

    def test_pylibmc(self):
        pylibmc = self.mock_pylibmc()
        pylibmc.next()
        from celery.backends import cache
        cache._imp = [None]
        self.assertEqual(cache.get_best_memcache().__module__, "pylibmc")
        pylibmc.next()

    def xxx_memcache(self):

        def with_no_pylibmc():
            from celery.backends import cache
            cache._imp = [None]
            self.assertEqual(cache.get_best_memcache().__module__, "memcache")

        context = mask_modules("pylibmc")
        context.__enter__()
        try:
            memcache = self.mock_memcache()
            memcache.next()
            with_no_pylibmc()
            memcache.next()
        finally:
            context.__exit__(None, None, None)

    def xxx_no_implementations(self):

        def with_no_memcache_libs():
            from celery.backends import cache
            cache._imp = [None]
            self.assertRaises(ImproperlyConfigured, cache.get_best_memcache)

        context = mask_modules("pylibmc", "memcache")
        context.__enter__()
        try:
            with_no_memcache_libs()
        finally:
            context.__exit__(None, None, None)
