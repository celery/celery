from __future__ import absolute_import
from __future__ import with_statement

import sys
import types

from contextlib import contextmanager

from mock import Mock, patch

from celery import states
from celery.backends.cache import CacheBackend, DummyClient
from celery.exceptions import ImproperlyConfigured
from celery.registry import tasks
from celery.result import AsyncResult
from celery.task import subtask
from celery.utils import uuid
from celery.utils.encoding import str_to_bytes

from celery.tests.utils import Case, mask_modules, reset_modules


class SomeClass(object):

    def __init__(self, data):
        self.data = data


class test_CacheBackend(Case):

    def setUp(self):
        self.tb = CacheBackend(backend="memory://")
        self.tid = uuid()

    def test_mark_as_done(self):
        self.assertEqual(self.tb.get_status(self.tid), states.PENDING)
        self.assertIsNone(self.tb.get_result(self.tid))

        self.tb.mark_as_done(self.tid, 42)
        self.assertEqual(self.tb.get_status(self.tid), states.SUCCESS)
        self.assertEqual(self.tb.get_result(self.tid), 42)

    def test_is_pickled(self):
        result = {"foo": "baz", "bar": SomeClass(12345)}
        self.tb.mark_as_done(self.tid, result)
        # is serialized properly.
        rindb = self.tb.get_result(self.tid)
        self.assertEqual(rindb.get("foo"), "baz")
        self.assertEqual(rindb.get("bar").data, 12345)

    def test_mark_as_failure(self):
        try:
            raise KeyError("foo")
        except KeyError, exception:
            self.tb.mark_as_failure(self.tid, exception)
            self.assertEqual(self.tb.get_status(self.tid), states.FAILURE)
            self.assertIsInstance(self.tb.get_result(self.tid), KeyError)

    def test_on_chord_apply(self):
        tb = CacheBackend(backend="memory://")
        tb.on_chord_apply("setid", [])

    @patch("celery.result.TaskSetResult")
    def test_on_chord_part_return(self, setresult):
        tb = CacheBackend(backend="memory://")

        deps = Mock()
        deps.total = 2
        setresult.restore.return_value = deps
        task = Mock()
        task.name = "foobarbaz"
        try:
            tasks["foobarbaz"] = task
            task.request.chord = subtask(task)
            task.request.taskset = "setid"

            tb.on_chord_apply(task.request.taskset, [])

            self.assertFalse(deps.join.called)
            tb.on_chord_part_return(task)
            self.assertFalse(deps.join.called)

            tb.on_chord_part_return(task)
            deps.join.assert_called_with(propagate=False)
            deps.delete.assert_called_with()

        finally:
            tasks.pop("foobarbaz")

    def test_mget(self):
        self.tb.set("foo", 1)
        self.tb.set("bar", 2)

        self.assertDictEqual(self.tb.mget(["foo", "bar"]),
                             {"foo": 1, "bar": 2})

    def test_forget(self):
        self.tb.mark_as_done(self.tid, {"foo": "bar"})
        x = AsyncResult(self.tid, backend=self.tb)
        x.forget()
        self.assertIsNone(x.result)

    def test_process_cleanup(self):
        self.tb.process_cleanup()

    def test_expires_as_int(self):
        tb = CacheBackend(backend="memory://", expires=10)
        self.assertEqual(tb.expires, 10)

    def test_unknown_backend_raises_ImproperlyConfigured(self):
        with self.assertRaises(ImproperlyConfigured):
            CacheBackend(backend="unknown://")


class MyMemcachedStringEncodingError(Exception):
    pass


class MemcachedClient(DummyClient):

    def set(self, key, value, *args, **kwargs):
        if isinstance(key, unicode):
            raise MyMemcachedStringEncodingError(
                    "Keys must be str()'s, not unicode.  Convert your unicode "
                    "strings using mystring.encode(charset)!")
        return super(MemcachedClient, self).set(key, value, *args, **kwargs)


class MockCacheMixin(object):

    @contextmanager
    def mock_memcache(self):
        memcache = types.ModuleType("memcache")
        memcache.Client = MemcachedClient
        memcache.Client.__module__ = memcache.__name__
        prev, sys.modules["memcache"] = sys.modules.get("memcache"), memcache
        yield True
        if prev is not None:
            sys.modules["memcache"] = prev

    @contextmanager
    def mock_pylibmc(self):
        pylibmc = types.ModuleType("pylibmc")
        pylibmc.Client = MemcachedClient
        pylibmc.Client.__module__ = pylibmc.__name__
        prev = sys.modules.get("pylibmc")
        sys.modules["pylibmc"] = pylibmc
        yield True
        if prev is not None:
            sys.modules["pylibmc"] = prev


class test_get_best_memcache(Case, MockCacheMixin):

    def test_pylibmc(self):
        with self.mock_pylibmc():
            with reset_modules("celery.backends.cache"):
                from celery.backends import cache
                cache._imp = [None]
                self.assertEqual(cache.get_best_memcache().__module__,
                                 "pylibmc")

    def test_memcache(self):
        with self.mock_memcache():
            with reset_modules("celery.backends.cache"):
                with mask_modules("pylibmc"):
                    from celery.backends import cache
                    cache._imp = [None]
                    self.assertEqual(cache.get_best_memcache().__module__,
                                     "memcache")

    def test_no_implementations(self):
        with mask_modules("pylibmc", "memcache"):
            with reset_modules("celery.backends.cache"):
                from celery.backends import cache
                cache._imp = [None]
                with self.assertRaises(ImproperlyConfigured):
                    cache.get_best_memcache()

    def test_cached(self):
        with self.mock_pylibmc():
            with reset_modules("celery.backends.cache"):
                from celery.backends import cache
                cache._imp = [None]
                cache.get_best_memcache(behaviors={"foo": "bar"})
                self.assertTrue(cache._imp[0])
                cache.get_best_memcache()

    def test_backends(self):
        from celery.backends.cache import backends
        for name, fun in backends.items():
            self.assertTrue(fun())


class test_memcache_key(Case, MockCacheMixin):

    def test_memcache_unicode_key(self):
        with self.mock_memcache():
            with reset_modules("celery.backends.cache"):
                with mask_modules("pylibmc"):
                    from celery.backends import cache
                    cache._imp = [None]
                    task_id, result = unicode(uuid()), 42
                    b = cache.CacheBackend(backend='memcache')
                    b.store_result(task_id, result, status=states.SUCCESS)
                    self.assertEqual(b.get_result(task_id), result)

    def test_memcache_bytes_key(self):
        with self.mock_memcache():
            with reset_modules("celery.backends.cache"):
                with mask_modules("pylibmc"):
                    from celery.backends import cache
                    cache._imp = [None]
                    task_id, result = str_to_bytes(uuid()), 42
                    b = cache.CacheBackend(backend='memcache')
                    b.store_result(task_id, result, status=states.SUCCESS)
                    self.assertEqual(b.get_result(task_id), result)

    def test_pylibmc_unicode_key(self):
        with reset_modules("celery.backends.cache"):
            with self.mock_pylibmc():
                from celery.backends import cache
                cache._imp = [None]
                task_id, result = unicode(uuid()), 42
                b = cache.CacheBackend(backend='memcache')
                b.store_result(task_id, result, status=states.SUCCESS)
                self.assertEqual(b.get_result(task_id), result)

    def test_pylibmc_bytes_key(self):
        with reset_modules("celery.backends.cache"):
            with self.mock_pylibmc():
                from celery.backends import cache
                cache._imp = [None]
                task_id, result = str_to_bytes(uuid()), 42
                b = cache.CacheBackend(backend='memcache')
                b.store_result(task_id, result, status=states.SUCCESS)
                self.assertEqual(b.get_result(task_id), result)
