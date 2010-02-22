import sys
import unittest

from billiard.serialization import pickle

from celery import result
from celery import states
from celery.utils import gen_unique_id
from celery.backends.cache import CacheBackend
from celery.datastructures import ExceptionInfo


class SomeClass(object):

    def __init__(self, data):
        self.data = data


class TestCacheBackend(unittest.TestCase):

    def test_mark_as_done(self):
        cb = CacheBackend()

        tid = gen_unique_id()

        self.assertFalse(cb.is_successful(tid))
        self.assertEquals(cb.get_status(tid), states.PENDING)
        self.assertEquals(cb.get_result(tid), None)

        cb.mark_as_done(tid, 42)
        self.assertTrue(cb.is_successful(tid))
        self.assertEquals(cb.get_status(tid), states.SUCCESS)
        self.assertEquals(cb.get_result(tid), 42)
        self.assertTrue(cb.get_result(tid), 42)

    def test_save_restore_taskset(self):
        backend = CacheBackend()
        taskset_id = gen_unique_id()
        subtask_ids = [gen_unique_id() for i in range(10)]
        subtasks = map(result.AsyncResult, subtask_ids)
        res = result.TaskSetResult(taskset_id, subtasks)
        res.save(backend=backend)
        saved = result.TaskSetResult.restore(taskset_id, backend=backend)
        self.assertEquals(saved.subtasks, subtasks)
        self.assertEquals(saved.taskset_id, taskset_id)

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

        einfo = None
        tid3 = gen_unique_id()
        try:
            raise KeyError("foo")
        except KeyError, exception:
            einfo = ExceptionInfo(sys.exc_info())
            pass
        cb.mark_as_failure(tid3, exception, traceback=einfo.traceback)
        self.assertFalse(cb.is_successful(tid3))
        self.assertEquals(cb.get_status(tid3), states.FAILURE)
        self.assertTrue(isinstance(cb.get_result(tid3), KeyError))
        self.assertEquals(cb.get_traceback(tid3), einfo.traceback)

    def test_process_cleanup(self):
        cb = CacheBackend()

        cb.process_cleanup()


class TestCustomCacheBackend(unittest.TestCase):

    def test_custom_cache_backend(self):
        from celery import conf
        prev_backend = conf.CELERY_CACHE_BACKEND
        prev_module = sys.modules["celery.backends.cache"]
        conf.CELERY_CACHE_BACKEND = "dummy://"
        sys.modules.pop("celery.backends.cache")
        try:
            from celery.backends.cache import cache
            from django.core.cache import cache as django_cache
            self.assertEquals(cache.__class__.__module__,
                              "django.core.cache.backends.dummy")
            self.assertTrue(cache is not django_cache)
        finally:
            conf.CELERY_CACHE_BACKEND = prev_backend
            sys.modules["celery.backends.cache"] = prev_module


class TestMemcacheWrapper(unittest.TestCase):

    def test_memcache_wrapper(self):

        from django.core.cache.backends import memcached
        from django.core.cache.backends import locmem
        prev_cache_cls = memcached.CacheClass
        memcached.CacheClass = locmem.CacheClass
        prev_backend_module = sys.modules.pop("celery.backends.cache")
        try:
            from celery.backends.cache import cache, DjangoMemcacheWrapper
            self.assertTrue(isinstance(cache, DjangoMemcacheWrapper))

            key = "cu.test_memcache_wrapper"
            val = "The quick brown fox."
            default = "The lazy dog."

            self.assertEquals(cache.get(key, default=default), default)
            cache.set(key, val)
            self.assertEquals(pickle.loads(cache.get(key, default=default)),
                              val)
        finally:
            memcached.CacheClass = prev_cache_cls
            sys.modules["celery.backends.cache"] = prev_backend_module
