import sys
import types
from contextlib import contextmanager
from unittest.mock import Mock, patch

import pytest
from case import mock
from kombu.utils.encoding import ensure_bytes, str_to_bytes

from celery import signature, states, uuid
from celery.backends.cache import CacheBackend, DummyClient, backends
from celery.exceptions import ImproperlyConfigured


class SomeClass:

    def __init__(self, data):
        self.data = data


class test_CacheBackend:

    def setup(self):
        self.app.conf.result_serializer = 'pickle'
        self.tb = CacheBackend(backend='memory://', app=self.app)
        self.tid = uuid()
        self.old_get_best_memcached = backends['memcache']
        backends['memcache'] = lambda: (DummyClient, ensure_bytes)

    def teardown(self):
        backends['memcache'] = self.old_get_best_memcached

    def test_no_backend(self):
        self.app.conf.cache_backend = None
        with pytest.raises(ImproperlyConfigured):
            CacheBackend(backend=None, app=self.app)

    def test_memory_client_is_shared(self):
        """This test verifies that memory:// backend state is shared over multiple threads"""
        from threading import Thread
        t = Thread(
            target=lambda: CacheBackend(backend='memory://', app=self.app).set('test', 12345)
        )
        t.start()
        t.join()
        assert self.tb.client.get('test') == 12345

    def test_mark_as_done(self):
        assert self.tb.get_state(self.tid) == states.PENDING
        assert self.tb.get_result(self.tid) is None

        self.tb.mark_as_done(self.tid, 42)
        assert self.tb.get_state(self.tid) == states.SUCCESS
        assert self.tb.get_result(self.tid) == 42

    def test_is_pickled(self):
        result = {'foo': 'baz', 'bar': SomeClass(12345)}
        self.tb.mark_as_done(self.tid, result)
        # is serialized properly.
        rindb = self.tb.get_result(self.tid)
        assert rindb.get('foo') == 'baz'
        assert rindb.get('bar').data == 12345

    def test_mark_as_failure(self):
        try:
            raise KeyError('foo')
        except KeyError as exception:
            self.tb.mark_as_failure(self.tid, exception)
            assert self.tb.get_state(self.tid) == states.FAILURE
            assert isinstance(self.tb.get_result(self.tid), KeyError)

    def test_apply_chord(self):
        tb = CacheBackend(backend='memory://', app=self.app)
        result = self.app.GroupResult(
            uuid(),
            [self.app.AsyncResult(uuid()) for _ in range(3)],
        )
        tb.apply_chord(result, None)
        assert self.app.GroupResult.restore(result.id, backend=tb) == result

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

        result = self.app.GroupResult(
            uuid(),
            [self.app.AsyncResult(uuid()) for _ in range(3)],
        )
        task.request.group = result.id
        tb.apply_chord(result, None)

        deps.join_native.assert_not_called()
        tb.on_chord_part_return(task.request, 'SUCCESS', 10)
        deps.join_native.assert_not_called()

        tb.on_chord_part_return(task.request, 'SUCCESS', 10)
        deps.join_native.assert_called_with(propagate=True, timeout=3.0)
        deps.delete.assert_called_with()

    def test_mget(self):
        self.tb._set_with_state('foo', 1, states.SUCCESS)
        self.tb._set_with_state('bar', 2, states.SUCCESS)

        assert self.tb.mget(['foo', 'bar']) == {'foo': 1, 'bar': 2}

    def test_forget(self):
        self.tb.mark_as_done(self.tid, {'foo': 'bar'})
        x = self.app.AsyncResult(self.tid, backend=self.tb)
        x.forget()
        assert x.result is None

    def test_process_cleanup(self):
        self.tb.process_cleanup()

    def test_expires_as_int(self):
        tb = CacheBackend(backend='memory://', expires=10, app=self.app)
        assert tb.expires == 10

    def test_unknown_backend_raises_ImproperlyConfigured(self):
        with pytest.raises(ImproperlyConfigured):
            CacheBackend(backend='unknown://', app=self.app)

    def test_as_uri_no_servers(self):
        assert self.tb.as_uri() == 'memory:///'

    def test_as_uri_one_server(self):
        backend = 'memcache://127.0.0.1:11211/'
        b = CacheBackend(backend=backend, app=self.app)
        assert b.as_uri() == backend

    def test_as_uri_multiple_servers(self):
        backend = 'memcache://127.0.0.1:11211;127.0.0.2:11211;127.0.0.3/'
        b = CacheBackend(backend=backend, app=self.app)
        assert b.as_uri() == backend

    def test_regression_worker_startup_info(self):
        pytest.importorskip('memcached')
        self.app.conf.result_backend = (
            'cache+memcached://127.0.0.1:11211;127.0.0.2:11211;127.0.0.3/'
        )
        worker = self.app.Worker()
        with mock.stdouts():
            worker.on_start()
            assert worker.startup_info()


class MyMemcachedStringEncodingError(Exception):
    pass


class MemcachedClient(DummyClient):

    def set(self, key, value, *args, **kwargs):
        key_t, must_be, not_be, cod = bytes, 'string', 'bytes', 'decode'

        if isinstance(key, key_t):
            raise MyMemcachedStringEncodingError(
                f'Keys must be {must_be}, not {not_be}.  Convert your '
                f'strings using mystring.{cod}(charset)!')
        return super().set(key, value, *args, **kwargs)


class MockCacheMixin:

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


class test_get_best_memcache(MockCacheMixin):

    def test_pylibmc(self):
        with self.mock_pylibmc():
            with mock.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                cache._imp = [None]
                assert cache.get_best_memcache()[0].__module__ == 'pylibmc'

    def test_memcache(self):
        with self.mock_memcache():
            with mock.reset_modules('celery.backends.cache'):
                with mock.mask_modules('pylibmc'):
                    from celery.backends import cache
                    cache._imp = [None]
                    assert (cache.get_best_memcache()[0]().__module__ ==
                            'memcache')

    def test_no_implementations(self):
        with mock.mask_modules('pylibmc', 'memcache'):
            with mock.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                cache._imp = [None]
                with pytest.raises(ImproperlyConfigured):
                    cache.get_best_memcache()

    def test_cached(self):
        with self.mock_pylibmc():
            with mock.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                cache._imp = [None]
                cache.get_best_memcache()[0](behaviors={'foo': 'bar'})
                assert cache._imp[0]
                cache.get_best_memcache()[0]()

    def test_backends(self):
        from celery.backends.cache import backends
        with self.mock_memcache():
            for name, fun in backends.items():
                assert fun()


class test_memcache_key(MockCacheMixin):

    def test_memcache_unicode_key(self):
        with self.mock_memcache():
            with mock.reset_modules('celery.backends.cache'):
                with mock.mask_modules('pylibmc'):
                    from celery.backends import cache
                    cache._imp = [None]
                    task_id, result = str(uuid()), 42
                    b = cache.CacheBackend(backend='memcache', app=self.app)
                    b.store_result(task_id, result, state=states.SUCCESS)
                    assert b.get_result(task_id) == result

    def test_memcache_bytes_key(self):
        with self.mock_memcache():
            with mock.reset_modules('celery.backends.cache'):
                with mock.mask_modules('pylibmc'):
                    from celery.backends import cache
                    cache._imp = [None]
                    task_id, result = str_to_bytes(uuid()), 42
                    b = cache.CacheBackend(backend='memcache', app=self.app)
                    b.store_result(task_id, result, state=states.SUCCESS)
                    assert b.get_result(task_id) == result

    def test_pylibmc_unicode_key(self):
        with mock.reset_modules('celery.backends.cache'):
            with self.mock_pylibmc():
                from celery.backends import cache
                cache._imp = [None]
                task_id, result = str(uuid()), 42
                b = cache.CacheBackend(backend='memcache', app=self.app)
                b.store_result(task_id, result, state=states.SUCCESS)
                assert b.get_result(task_id) == result

    def test_pylibmc_bytes_key(self):
        with mock.reset_modules('celery.backends.cache'):
            with self.mock_pylibmc():
                from celery.backends import cache
                cache._imp = [None]
                task_id, result = str_to_bytes(uuid()), 42
                b = cache.CacheBackend(backend='memcache', app=self.app)
                b.store_result(task_id, result, state=states.SUCCESS)
                assert b.get_result(task_id) == result
