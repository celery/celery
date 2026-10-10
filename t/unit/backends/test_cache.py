import sys
import types
from contextlib import contextmanager
from unittest.mock import Mock, call, patch

import pytest
from kombu.utils.encoding import bytes_to_str, ensure_bytes, str_to_bytes

from celery import signature, states, uuid
from celery.backends.cache import CacheBackend, DummyClient, backends
from celery.exceptions import CDeprecationWarning, ImproperlyConfigured
from t.unit import conftest


class SomeClass:

    def __init__(self, data):
        self.data = data


class test_CacheBackend:

    def setup_method(self):
        self.app.conf.result_serializer = 'pickle'
        self.tb = CacheBackend(backend='memory://', app=self.app)
        self.tid = uuid()
        self.old_get_memcache_client = backends['memcache']
        backends['memcache'] = lambda: (DummyClient, ensure_bytes)

    def teardown_method(self):
        backends['memcache'] = self.old_get_memcache_client

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
        result_args = (
            uuid(),
            [self.app.AsyncResult(uuid()) for _ in range(3)],
        )
        tb.apply_chord(result_args, None)
        assert self.app.GroupResult.restore(result_args[0], backend=tb) == self.app.GroupResult(*result_args)

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

        result_args = (
            uuid(),
            [self.app.AsyncResult(uuid()) for _ in range(3)],
        )
        task.request.id = uuid()
        task.request.group = result_args[0]
        tb.apply_chord(result_args, None)

        deps.join_native.assert_not_called()
        tb.on_chord_part_return(task.request, 'SUCCESS', 10)
        deps.join_native.assert_not_called()

        tb.on_chord_part_return(task.request, 'SUCCESS', 10)
        deps.join_native.assert_called_with(propagate=True, timeout=3.0)
        deps.delete.assert_called_with()

    @pytest.mark.parametrize(
        'result_expires,chord_expires,expected_chord,expected_result', [
            (100, 600, 600, 600),
            (86400, 600, 600, 86400),
            (100, None, 100, 100),
            (100, 0, 0, 0),
            (None, 600, 600, 0),
        ])
    @patch('celery.result.GroupResult.restore')
    def test_chord_uses_chord_expires(self, restore, result_expires,
                                      chord_expires, expected_chord,
                                      expected_result):
        self.app.conf.result_expires = result_expires
        self.app.conf.result_chord_expires = chord_expires
        tb = CacheBackend(backend='memory://', app=self.app)
        restore.return_value.__len__ = Mock(return_value=2)
        task = Mock()
        task.request.id = uuid()
        task.request.group = gid = uuid()
        task.request.chord = {}
        with patch.object(tb.client, 'set') as client_set, \
                patch.object(tb, 'expire') as expire:
            tb.apply_chord((gid, []), None)
        client_set.assert_any_call(
            tb.get_key_for_chord(gid), 0, expected_chord or 0)
        expire.assert_called_once_with(
            tb.get_key_for_group(gid), expected_chord)
        tb.client.set(tb.get_key_for_chord(gid), 0)
        with patch.object(tb, 'expire') as expire:
            tb.on_chord_part_return(task.request, states.SUCCESS, 1)
        expire.assert_has_calls([
            call(tb.get_key_for_chord(gid), expected_chord),
            call(tb.get_key_for_group(gid), expected_chord),
            call(tb.get_key_for_task(task.request.id), expected_result),
        ])

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


def _check_expire(expire):
    # pymemcache rejects anything but an integer expiry (e.g. None).
    if not isinstance(expire, int):
        raise TypeError(f'expire must be an integer, got {expire!r}')


def make_pymemcache_clients():
    """Build mock clients that keep the real pymemcache method signatures."""
    utils = pytest.importorskip('pymemcache.test.utils')
    retrying = pytest.importorskip('pymemcache.client.retrying')

    class PyMemcacheClient(utils.MockMemcacheClient):
        """Mock pymemcache base Client."""
        __module__ = 'pymemcache.client.base'

        def __init__(self, server, **kwargs):
            super().__init__(server, **kwargs)
            self.server = server
            self.options = kwargs

        def set(self, key, value, expire=0, noreply=None, flags=None):
            _check_expire(expire)
            return super().set(key, value, expire, noreply, flags)

        def touch(self, key, expire=0, noreply=None):
            _check_expire(expire)
            return super().touch(key, expire, noreply)

    class PyMemcacheHashClient(PyMemcacheClient):
        """Mock pymemcache HashClient."""
        __module__ = 'pymemcache.client.hash'

        def __init__(self, servers, **kwargs):
            super().__init__(None, **kwargs)
            self.servers = servers

    return PyMemcacheClient, PyMemcacheHashClient, retrying.RetryingClient


class MockPyMemcacheMixin:

    @contextmanager
    def mock_pymemcache(self):
        """Mock pymemcache modules."""
        Client, HashClient, RetryingClient = make_pymemcache_clients()

        pymemcache = types.ModuleType('pymemcache')
        pymemcache_client = types.ModuleType('pymemcache.client')
        pymemcache_client_base = types.ModuleType('pymemcache.client.base')
        pymemcache_client_hash = types.ModuleType('pymemcache.client.hash')
        pymemcache_client_retrying = types.ModuleType('pymemcache.client.retrying')

        # Parent modules must be packages for submodule imports to work
        pymemcache.__path__ = []
        pymemcache_client.__path__ = []

        pymemcache_client_base.Client = Client
        pymemcache_client_hash.HashClient = HashClient
        pymemcache_client_retrying.RetryingClient = RetryingClient
        pymemcache.Client = Client

        pymemcache_client.base = pymemcache_client_base
        pymemcache_client.hash = pymemcache_client_hash
        pymemcache_client.retrying = pymemcache_client_retrying
        pymemcache.client = pymemcache_client

        # Save previous state
        prev_modules = {
            'pymemcache': sys.modules.get('pymemcache'),
            'pymemcache.client': sys.modules.get('pymemcache.client'),
            'pymemcache.client.base': sys.modules.get('pymemcache.client.base'),
            'pymemcache.client.hash': sys.modules.get('pymemcache.client.hash'),
            'pymemcache.client.retrying': sys.modules.get('pymemcache.client.retrying'),
        }

        # Install mocks
        sys.modules['pymemcache'] = pymemcache
        sys.modules['pymemcache.client'] = pymemcache_client
        sys.modules['pymemcache.client.base'] = pymemcache_client_base
        sys.modules['pymemcache.client.hash'] = pymemcache_client_hash
        sys.modules['pymemcache.client.retrying'] = pymemcache_client_retrying

        try:
            yield True
        finally:
            # Clean up mocks
            sys.modules.pop('pymemcache', None)
            sys.modules.pop('pymemcache.client', None)
            sys.modules.pop('pymemcache.client.base', None)
            sys.modules.pop('pymemcache.client.hash', None)
            sys.modules.pop('pymemcache.client.retrying', None)

            # Restore previous modules
            for name, module in prev_modules.items():
                if module is not None:
                    sys.modules[name] = module


class test_pymemcache_client(MockPyMemcacheMixin):

    def test_single_server_uses_base_client(self):
        """Test that single server uses base Client."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                Client, _ = cache.get_memcache_client()
                client = Client(['127.0.0.1:11211'])
                assert client.__module__ == 'pymemcache.client.base'

    def test_multiple_servers_uses_hash_client(self):
        """Test that multiple servers use HashClient."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                Client, _ = cache.get_memcache_client()
                client = Client(['127.0.0.1:11211', '127.0.0.1:11212'])
                assert client.__module__ == 'pymemcache.client.hash'

    def test_retry_client_wrapping(self):
        """Test that retry_attempts enables RetryingClient."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                Client, _ = cache.get_memcache_client()
                client = Client(['127.0.0.1:11211'], retry_attempts=3, retry_delay=0.1)
                assert client.__module__ == 'pymemcache.client.retrying'
                assert client._attempts == 3
                assert client._retry_delay == 0.1

    def test_retry_client_with_exceptions(self):
        """Test that retry options are properly passed."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                Client, _ = cache.get_memcache_client()
                retry_for = [Exception]
                do_not_retry_for = [KeyError]
                client = Client(
                    ['127.0.0.1:11211'],
                    retry_attempts=2,
                    retry_for=retry_for,
                    do_not_retry_for=do_not_retry_for
                )
                assert client.__module__ == 'pymemcache.client.retrying'
                assert client._retry_for == tuple(retry_for)
                assert client._do_not_retry_for == tuple(do_not_retry_for)

    @pytest.mark.parametrize('option, value', [
        ('behaviors', {'tcp_nodelay': True}),
        ('binary', True),
    ])
    def test_pylibmc_options_ignored_with_warning(self, option, value):
        """Test that pylibmc only options are dropped with a warning."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                Client, _ = cache.get_memcache_client()
                with pytest.warns(CDeprecationWarning, match=option):
                    client = Client(['127.0.0.1:11211'], **{option: value})
                assert client.__module__ == 'pymemcache.client.base'
                assert option not in client.options

    @pytest.mark.parametrize('servers', [
        ['127.0.0.1:11211'],
        ['127.0.0.1:11211', '127.0.0.1:11212'],
    ])
    def test_default_client_options(self, servers):
        """Test that replies are awaited and timeouts are set by default."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                Client, _ = cache.get_memcache_client()
                client = Client(servers)
                assert client.options == cache.DEFAULT_CLIENT_OPTIONS

    def test_default_client_options_can_be_overridden(self):
        """Test that user options take precedence over the defaults."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                Client, _ = cache.get_memcache_client()
                client = Client(
                    ['127.0.0.1:11211'], default_noreply=True, timeout=None,
                )
                assert client.options['default_noreply'] is True
                assert client.options['timeout'] is None
                assert client.options['connect_timeout'] == 5.0

    def test_deprecated_get_best_memcache(self):
        """Test that get_best_memcache() still works with a warning."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                with pytest.warns(CDeprecationWarning, match='get_best_memcache'):
                    Client, key_t = cache.get_best_memcache()
                client = Client(['127.0.0.1:11211'])
                assert client.__module__ == 'pymemcache.client.base'
                assert key_t is bytes_to_str

    def test_deprecated_import_best_memcache(self):
        """Test that import_best_memcache() still works with a warning."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                with pytest.warns(CDeprecationWarning, match='import_best_memcache'):
                    is_pylibmc, memcache, key_t = cache.import_best_memcache()
                assert is_pylibmc is False
                assert memcache is sys.modules['pymemcache']
                assert key_t is bytes_to_str

    @pytest.mark.parametrize('servers', [None, [], [''], ('', '')])
    def test_no_servers_raises_error(self, servers):
        """Test that missing or empty servers raise ImproperlyConfigured."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                Client, _ = cache.get_memcache_client()
                with pytest.raises(ImproperlyConfigured):
                    Client(servers)

    def test_empty_server_entries_are_filtered(self):
        """Test that empty entries (e.g. trailing ';') are ignored."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                Client, _ = cache.get_memcache_client()
                client = Client(['127.0.0.1:11211', ''])
                assert client.__module__ == 'pymemcache.client.base'

    @pytest.mark.parametrize('servers', ['127.0.0.1:11211', ('127.0.0.1', 11211)])
    def test_single_server_address_is_accepted(self, servers):
        """Test that a single string or (host, port) address is accepted."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                Client, _ = cache.get_memcache_client()
                client = Client(servers)
                assert client.__module__ == 'pymemcache.client.base'

    @pytest.mark.parametrize('servers', [11211, [11211]])
    def test_invalid_servers_raises_error(self, servers):
        """Test that invalid server values raise ImproperlyConfigured."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                Client, _ = cache.get_memcache_client()
                with pytest.raises(ImproperlyConfigured):
                    Client(servers)

    @pytest.mark.masked_modules(
        'pymemcache',
        'pymemcache.client.base',
        'pymemcache.client.hash',
        'pymemcache.client.retrying',
    )
    def test_no_pymemcache_raises_error(self, mask_modules):
        """Test that missing pymemcache raises ImproperlyConfigured."""
        with conftest.reset_modules('celery.backends.cache'):
            # Don't mock pymemcache - it's masked by the decorator
            from celery.backends import cache
            with pytest.raises(ImproperlyConfigured):
                cache.get_memcache_client()

    def test_all_backend_names_work(self):
        """Test that all backend names are mapped correctly."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                for backend_name in ['memcache', 'memcached', 'pylibmc', 'pymemcache']:
                    assert backend_name in cache.backends
                    Client, _ = cache.backends[backend_name]()
                    assert Client is not None


class test_pymemcache_integration(MockPyMemcacheMixin):

    def test_cache_backend_with_unicode_key(self):
        """Test storing and retrieving with unicode keys."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                task_id, result = str(uuid()), 42
                b = cache.CacheBackend(backend='memcache://127.0.0.1:11211/', app=self.app)
                b.store_result(task_id, result, state=states.SUCCESS)
                assert b.get_result(task_id) == result

    def test_cache_backend_with_bytes_key(self):
        """Test storing and retrieving with bytes keys."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                task_id, result = str_to_bytes(uuid()), 42
                b = cache.CacheBackend(backend='memcache://127.0.0.1:11211/', app=self.app)
                b.store_result(task_id, result, state=states.SUCCESS)
                assert b.get_result(task_id) == result

    def test_cache_backend_with_retry_options(self):
        """Test that retry options are passed to the client."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                self.app.conf.cache_backend_options = {
                    'retry_attempts': 3,
                    'retry_delay': 0.1,
                }
                b = cache.CacheBackend(backend='memcache://127.0.0.1:11211/', app=self.app)
                client = b.client
                # Should be wrapped with RetryingClient
                assert client.__module__ == 'pymemcache.client.retrying'
                assert client._attempts == 3
                assert client._retry_delay == 0.1

    def test_cache_backend_with_multiple_servers(self):
        """Test that multiple servers use HashClient."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                b = cache.CacheBackend(
                    backend='memcache://127.0.0.1:11211;127.0.0.1:11212/',
                    app=self.app
                )
                client = b.client
                assert client.__module__ == 'pymemcache.client.hash'

    def test_backward_compatible_pylibmc_backend_name(self):
        """Test that pylibmc:// backend name still works."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                b = cache.CacheBackend(backend='pylibmc://127.0.0.1:11211/', app=self.app)
                assert b.backend == 'pylibmc'
                client = b.client
                # Should use pymemcache under the hood
                assert client.__module__ == 'pymemcache.client.base'

    def test_cache_backend_without_servers(self):
        """Test that a URL without servers raises a clear error."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                b = cache.CacheBackend(backend='memcache://', app=self.app)
                with pytest.raises(ImproperlyConfigured):
                    b.client

    @patch('celery.result.GroupResult.restore')
    def test_chord_with_pymemcache_signatures(self, restore):
        """Test the chord counter with the real pymemcache signatures."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                b = cache.CacheBackend(backend='memcache://127.0.0.1:11211/', app=self.app)

                deps = Mock()
                deps.__len__ = Mock(return_value=2)
                restore.return_value = deps
                task = Mock()
                task.name = 'foobarbaz'
                self.app.tasks['foobarbaz'] = task
                task.request.chord = signature(task)

                result_args = (
                    uuid(),
                    [self.app.AsyncResult(uuid()) for _ in range(2)],
                )
                task.request.id = uuid()
                task.request.group = result_args[0]
                b.apply_chord(result_args, None)

                b.on_chord_part_return(task.request, 'SUCCESS', 10)
                deps.join_native.assert_not_called()

                b.on_chord_part_return(task.request, 'SUCCESS', 10)
                deps.join_native.assert_called_with(propagate=True, timeout=3.0)
                deps.delete.assert_called_with()

    def test_cache_backend_without_expiry(self):
        """Test that result_expires=None is sent as an integer to pymemcache."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                from celery.backends import cache
                self.app.conf.result_expires = None
                b = cache.CacheBackend(backend='memcache://127.0.0.1:11211/', app=self.app)
                assert b.expires is None
                task_id = uuid()
                b.store_result(task_id, 42, state=states.SUCCESS)
                assert b.get_result(task_id) == 42
                b.expire(task_id, b.expires)

    def test_regression_worker_startup_info(self):
        """Test that worker startup info works with multiple servers."""
        with self.mock_pymemcache():
            with conftest.reset_modules('celery.backends.cache'):
                self.app.conf.result_backend = (
                    'cache+memcached://127.0.0.1:11211;127.0.0.2:11211;127.0.0.3/'
                )
                worker = self.app.Worker()
                with conftest.stdouts():
                    worker.on_start()
                    assert worker.startup_info()
