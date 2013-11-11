from __future__ import absolute_import

import sys
import types

from contextlib import contextmanager

from kombu.utils.encoding import str_to_bytes

from celery import signature
from celery import states
from celery.backends.cache import CacheBackend, DummyClient
from celery.exceptions import ImproperlyConfigured
from celery.five import items, string, text_t
from celery.utils import uuid

from celery.tests.case import (
    AppCase, Mock, mask_modules, patch, reset_modules,
)


class SomeClass(object):

    def __init__(self, data):
        self.data = data


class test_CacheBackend(AppCase):

    def setup(self):
        self.tb = CacheBackend(backend='memory://', app=self.app)
        self.tid = uuid()

    def test_no_backend(self):
        self.app.conf.CELERY_CACHE_BACKEND = None
        with self.assertRaises(ImproperlyConfigured):
            CacheBackend(backend=None, app=self.app)

    def test_mark_as_done(self):
        self.assertEqual(self.tb.get_status(self.tid), states.PENDING)
        self.assertIsNone(self.tb.get_result(self.tid))

        self.tb.mark_as_done(self.tid, 42)
        self.assertEqual(self.tb.get_status(self.tid), states.SUCCESS)
        self.assertEqual(self.tb.get_result(self.tid), 42)

    def test_is_pickled(self):
        result = {'foo': 'baz', 'bar': SomeClass(12345)}
        self.tb.mark_as_done(self.tid, result)
        # is serialized properly.
        rindb = self.tb.get_result(self.tid)
        self.assertEqual(rindb.get('foo'), 'baz')
        self.assertEqual(rindb.get('bar').data, 12345)

    def test_mark_as_failure(self):
        try:
            raise KeyError('foo')
        except KeyError as exception:
            self.tb.mark_as_failure(self.tid, exception)
            self.assertEqual(self.tb.get_status(self.tid), states.FAILURE)
            self.assertIsInstance(self.tb.get_result(self.tid), KeyError)

    def test_on_chord_apply(self):
        tb = CacheBackend(backend='memory://', app=self.app)
        gid, res = uuid(), [self.app.AsyncResult(uuid()) for _ in range(3)]
        tb.on_chord_apply(gid, {}, result=res)

    @patch('celery.result.GroupResult.restore')
    def test_on_chord_part_return(self, restore):
        tb = CacheBackend(backend='memory://', app=self.app)

        deps = Mock()
        deps.__len__ = Mock()
        deps.__len__.return_value = 2
        restore.return_value = deps
        task = Mock()
        task.name = 'foobarbaz'
        self.app.tasks['foobarbaz'] = task
        task.request.chord = signature(task)

        gid, res = uuid(), [self.app.AsyncResult(uuid()) for _ in range(3)]
        task.request.group = gid
        tb.on_chord_apply(gid, {}, result=res)

        self.assertFalse(deps.join_native.called)
        tb.on_chord_part_return(task)
        self.assertFalse(deps.join_native.called)

        tb.on_chord_part_return(task)
        deps.join_native.assert_called_with(propagate=True)
        deps.delete.assert_called_with()

    def test_mget(self):
        self.tb.set('foo', 1)
        self.tb.set('bar', 2)

        self.assertDictEqual(self.tb.mget(['foo', 'bar']),
                             {'foo': 1, 'bar': 2})

    def test_forget(self):
        self.tb.mark_as_done(self.tid, {'foo': 'bar'})
        x = self.app.AsyncResult(self.tid, backend=self.tb)
        x.forget()
        self.assertIsNone(x.result)

    def test_process_cleanup(self):
        self.tb.process_cleanup()

    def test_expires_as_int(self):
        tb = CacheBackend(backend='memory://', expires=10, app=self.app)
        self.assertEqual(tb.expires, 10)

    def test_unknown_backend_raises_ImproperlyConfigured(self):
        with self.assertRaises(ImproperlyConfigured):
            CacheBackend(backend='unknown://', app=self.app)


class MyMemcachedStringEncodingError(Exception):
    pass


class MemcachedClient(DummyClient):

    def set(self, key, value, *args, **kwargs):
        if isinstance(key, text_t):
            raise MyMemcachedStringEncodingError(
                'Keys must be bytes, not string.  Convert your '
                'strings using mystring.encode(charset)!')
        return super(MemcachedClient, self).set(key, value, *args, **kwargs)


class MockCacheMixin(object):

    @contextmanager
    def mock_memcache(self):
        memcache = types.ModuleType('memcache')
        memcache.Client = MemcachedClient
        memcache.Client.__module__ = memcache.__name__
        prev, sys.modules['memcache'] = sys.modules.get('memcache'), memcache
        try:
            yield True
        finally:
            if prev is not None:
                sys.modules['memcache'] = prev

    @contextmanager
    def mock_pylibmc(self):
        pylibmc = types.ModuleType('pylibmc')
        pylibmc.Client = MemcachedClient
        pylibmc.Client.__module__ = pylibmc.__name__
        prev = sys.modules.get('pylibmc')
        sys.modules['pylibmc'] = pylibmc
        try:
            yield True
        finally:
            if prev is not None:
                sys.modules['pylibmc'] = prev


class test_get_best_memcache(AppCase, MockCacheMixin):

    def test_pylibmc(self):
        with self.mock_pylibmc():
            with reset_modules('celery.backends.cache'):
                from celery.backends import cache
                cache._imp = [None]
                self.assertEqual(cache.get_best_memcache().__module__,
                                 'pylibmc')

    def test_memcache(self):
        with self.mock_memcache():
            with reset_modules('celery.backends.cache'):
                with mask_modules('pylibmc'):
                    from celery.backends import cache
                    cache._imp = [None]
                    self.assertEqual(cache.get_best_memcache().__module__,
                                     'memcache')

    def test_no_implementations(self):
        with mask_modules('pylibmc', 'memcache'):
            with reset_modules('celery.backends.cache'):
                from celery.backends import cache
                cache._imp = [None]
                with self.assertRaises(ImproperlyConfigured):
                    cache.get_best_memcache()

    def test_cached(self):
        with self.mock_pylibmc():
            with reset_modules('celery.backends.cache'):
                from celery.backends import cache
                cache._imp = [None]
                cache.get_best_memcache(behaviors={'foo': 'bar'})
                self.assertTrue(cache._imp[0])
                cache.get_best_memcache()

    def test_backends(self):
        from celery.backends.cache import backends
        for name, fun in items(backends):
            self.assertTrue(fun())


class test_memcache_key(AppCase, MockCacheMixin):

    def test_memcache_unicode_key(self):
        with self.mock_memcache():
            with reset_modules('celery.backends.cache'):
                with mask_modules('pylibmc'):
                    from celery.backends import cache
                    cache._imp = [None]
                    task_id, result = string(uuid()), 42
                    b = cache.CacheBackend(backend='memcache', app=self.app)
                    b.store_result(task_id, result, status=states.SUCCESS)
                    self.assertEqual(b.get_result(task_id), result)

    def test_memcache_bytes_key(self):
        with self.mock_memcache():
            with reset_modules('celery.backends.cache'):
                with mask_modules('pylibmc'):
                    from celery.backends import cache
                    cache._imp = [None]
                    task_id, result = str_to_bytes(uuid()), 42
                    b = cache.CacheBackend(backend='memcache', app=self.app)
                    b.store_result(task_id, result, status=states.SUCCESS)
                    self.assertEqual(b.get_result(task_id), result)

    def test_pylibmc_unicode_key(self):
        with reset_modules('celery.backends.cache'):
            with self.mock_pylibmc():
                from celery.backends import cache
                cache._imp = [None]
                task_id, result = string(uuid()), 42
                b = cache.CacheBackend(backend='memcache', app=self.app)
                b.store_result(task_id, result, status=states.SUCCESS)
                self.assertEqual(b.get_result(task_id), result)

    def test_pylibmc_bytes_key(self):
        with reset_modules('celery.backends.cache'):
            with self.mock_pylibmc():
                from celery.backends import cache
                cache._imp = [None]
                task_id, result = str_to_bytes(uuid()), 42
                b = cache.CacheBackend(backend='memcache', app=self.app)
                b.store_result(task_id, result, status=states.SUCCESS)
                self.assertEqual(b.get_result(task_id), result)
