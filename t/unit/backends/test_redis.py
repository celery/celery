from __future__ import absolute_import, unicode_literals

import random
import ssl
from contextlib import contextmanager
from datetime import timedelta
from pickle import dumps, loads

import pytest
from case import ANY, ContextMock, Mock, call, mock, patch, skip

from celery import signature, states, uuid
from celery.canvas import Signature
from celery.exceptions import (ChordError, CPendingDeprecationWarning,
                               ImproperlyConfigured)
from celery.utils.collections import AttributeDict


def raise_on_second_call(mock, exc, *retval):
    def on_first_call(*args, **kwargs):
        mock.side_effect = exc
        return mock.return_value

    mock.side_effect = on_first_call
    if retval:
        mock.return_value, = retval


class Connection(object):
    connected = True

    def disconnect(self):
        self.connected = False


class Pipeline(object):
    def __init__(self, client):
        self.client = client
        self.steps = []

    def __getattr__(self, attr):
        def add_step(*args, **kwargs):
            self.steps.append((getattr(self.client, attr), args, kwargs))
            return self

        return add_step

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def execute(self):
        return [step(*a, **kw) for step, a, kw in self.steps]


class Redis(mock.MockCallbacks):
    Connection = Connection
    Pipeline = Pipeline

    def __init__(self, host=None, port=None, db=None, password=None, **kw):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.keyspace = {}
        self.expiry = {}
        self.connection = self.Connection()

    def get(self, key):
        return self.keyspace.get(key)

    def setex(self, key, expires, value):
        self.set(key, value)
        self.expire(key, expires)

    def set(self, key, value):
        self.keyspace[key] = value

    def expire(self, key, expires):
        self.expiry[key] = expires
        return expires

    def delete(self, key):
        return bool(self.keyspace.pop(key, None))

    def pipeline(self):
        return self.Pipeline(self)

    def _get_list(self, key):
        try:
            return self.keyspace[key]
        except KeyError:
            l = self.keyspace[key] = []
            return l

    def rpush(self, key, value):
        self._get_list(key).append(value)

    def lrange(self, key, start, stop):
        return self._get_list(key)[start:stop]

    def llen(self, key):
        return len(self.keyspace.get(key) or [])


class Sentinel(mock.MockCallbacks):
    def __init__(self, sentinels, min_other_sentinels=0, sentinel_kwargs=None,
                 **connection_kwargs):
        self.sentinel_kwargs = sentinel_kwargs
        self.sentinels = [Redis(hostname, port, **self.sentinel_kwargs)
                          for hostname, port in sentinels]
        self.min_other_sentinels = min_other_sentinels
        self.connection_kwargs = connection_kwargs

    def master_for(self, service_name, redis_class):
        return random.choice(self.sentinels)


class redis(object):
    StrictRedis = Redis

    class ConnectionPool(object):
        def __init__(self, **kwargs):
            pass

    class UnixDomainSocketConnection(object):
        def __init__(self, **kwargs):
            pass


class sentinel(object):
    Sentinel = Sentinel


class test_RedisBackend:
    def get_backend(self):
        from celery.backends.redis import RedisBackend

        class _RedisBackend(RedisBackend):
            redis = redis

        return _RedisBackend

    def get_E_LOST(self):
        from celery.backends.redis import E_LOST
        return E_LOST

    def setup(self):
        self.Backend = self.get_backend()
        self.E_LOST = self.get_E_LOST()
        self.b = self.Backend(app=self.app)

    @pytest.mark.usefixtures('depends_on_current_app')
    @skip.unless_module('redis')
    def test_reduce(self):
        from celery.backends.redis import RedisBackend
        x = RedisBackend(app=self.app)
        assert loads(dumps(x))

    def test_no_redis(self):
        self.Backend.redis = None
        with pytest.raises(ImproperlyConfigured):
            self.Backend(app=self.app)

    def test_url(self):
        self.app.conf.redis_socket_timeout = 30.0
        self.app.conf.redis_socket_connect_timeout = 100.0
        x = self.Backend(
            'redis://:bosco@vandelay.com:123//1', app=self.app,
        )
        assert x.connparams
        assert x.connparams['host'] == 'vandelay.com'
        assert x.connparams['db'] == 1
        assert x.connparams['port'] == 123
        assert x.connparams['password'] == 'bosco'
        assert x.connparams['socket_timeout'] == 30.0
        assert x.connparams['socket_connect_timeout'] == 100.0

    def test_socket_url(self):
        self.app.conf.redis_socket_timeout = 30.0
        self.app.conf.redis_socket_connect_timeout = 100.0
        x = self.Backend(
            'socket:///tmp/redis.sock?virtual_host=/3', app=self.app,
        )
        assert x.connparams
        assert x.connparams['path'] == '/tmp/redis.sock'
        assert (x.connparams['connection_class'] is
                redis.UnixDomainSocketConnection)
        assert 'host' not in x.connparams
        assert 'port' not in x.connparams
        assert x.connparams['socket_timeout'] == 30.0
        assert 'socket_connect_timeout' not in x.connparams
        assert x.connparams['db'] == 3

    @skip.unless_module('redis')
    def test_backend_ssl(self):
        self.app.conf.redis_backend_use_ssl = {
            'ssl_cert_reqs': ssl.CERT_REQUIRED,
            'ssl_ca_certs': '/path/to/ca.crt',
            'ssl_certfile': '/path/to/client.crt',
            'ssl_keyfile': '/path/to/client.key',
        }
        self.app.conf.redis_socket_timeout = 30.0
        self.app.conf.redis_socket_connect_timeout = 100.0
        x = self.Backend(
            'redis://:bosco@vandelay.com:123//1', app=self.app,
        )
        assert x.connparams
        assert x.connparams['host'] == 'vandelay.com'
        assert x.connparams['db'] == 1
        assert x.connparams['port'] == 123
        assert x.connparams['password'] == 'bosco'
        assert x.connparams['socket_timeout'] == 30.0
        assert x.connparams['socket_connect_timeout'] == 100.0
        assert x.connparams['ssl_cert_reqs'] == ssl.CERT_REQUIRED
        assert x.connparams['ssl_ca_certs'] == '/path/to/ca.crt'
        assert x.connparams['ssl_certfile'] == '/path/to/client.crt'
        assert x.connparams['ssl_keyfile'] == '/path/to/client.key'

        from redis.connection import SSLConnection
        assert x.connparams['connection_class'] is SSLConnection

    def test_compat_propertie(self):
        x = self.Backend(
            'redis://:bosco@vandelay.com:123//1', app=self.app,
        )
        with pytest.warns(CPendingDeprecationWarning):
            assert x.host == 'vandelay.com'
        with pytest.warns(CPendingDeprecationWarning):
            assert x.db == 1
        with pytest.warns(CPendingDeprecationWarning):
            assert x.port == 123
        with pytest.warns(CPendingDeprecationWarning):
            assert x.password == 'bosco'

    def test_conf_raises_KeyError(self):
        self.app.conf = AttributeDict({
            'result_serializer': 'json',
            'result_cache_max': 1,
            'result_expires': None,
            'accept_content': ['json'],
        })
        self.Backend(app=self.app)

    @patch('celery.backends.redis.logger')
    def test_on_connection_error(self, logger):
        intervals = iter([10, 20, 30])
        exc = KeyError()
        assert self.b.on_connection_error(None, exc, intervals, 1) == 10
        logger.error.assert_called_with(
            self.E_LOST, 1, 'Inf', 'in 10.00 seconds')
        assert self.b.on_connection_error(10, exc, intervals, 2) == 20
        logger.error.assert_called_with(self.E_LOST, 2, 10, 'in 20.00 seconds')
        assert self.b.on_connection_error(10, exc, intervals, 3) == 30
        logger.error.assert_called_with(self.E_LOST, 3, 10, 'in 30.00 seconds')

    def test_incr(self):
        self.b.client = Mock(name='client')
        self.b.incr('foo')
        self.b.client.incr.assert_called_with('foo')

    def test_expire(self):
        self.b.client = Mock(name='client')
        self.b.expire('foo', 300)
        self.b.client.expire.assert_called_with('foo', 300)

    def test_apply_chord(self, unlock='celery.chord_unlock'):
        self.app.tasks[unlock] = Mock()
        header_result = self.app.GroupResult(
            uuid(),
            [self.app.AsyncResult(x) for x in range(3)],
        )
        self.b.apply_chord(header_result, None)
        assert self.app.tasks[unlock].apply_async.call_count == 0

    def test_unpack_chord_result(self):
        self.b.exception_to_python = Mock(name='etp')
        decode = Mock(name='decode')
        exc = KeyError()
        tup = decode.return_value = (1, 'id1', states.FAILURE, exc)
        with pytest.raises(ChordError):
            self.b._unpack_chord_result(tup, decode)
        decode.assert_called_with(tup)
        self.b.exception_to_python.assert_called_with(exc)

        exc = ValueError()
        tup = decode.return_value = (2, 'id2', states.RETRY, exc)
        ret = self.b._unpack_chord_result(tup, decode)
        self.b.exception_to_python.assert_called_with(exc)
        assert ret is self.b.exception_to_python()

    def test_on_chord_part_return_no_gid_or_tid(self):
        request = Mock(name='request')
        request.id = request.group = None
        assert self.b.on_chord_part_return(request, 'SUCCESS', 10) is None

    def test_ConnectionPool(self):
        self.b.redis = Mock(name='redis')
        assert self.b._ConnectionPool is None
        assert self.b.ConnectionPool is self.b.redis.ConnectionPool
        assert self.b.ConnectionPool is self.b.redis.ConnectionPool

    def test_expires_defaults_to_config(self):
        self.app.conf.result_expires = 10
        b = self.Backend(expires=None, app=self.app)
        assert b.expires == 10

    def test_expires_is_int(self):
        b = self.Backend(expires=48, app=self.app)
        assert b.expires == 48

    def test_add_to_chord(self):
        b = self.Backend('redis://', app=self.app)
        gid = uuid()
        b.add_to_chord(gid, 'sig')
        b.client.incr.assert_called_with(b.get_key_for_group(gid, '.t'), 1)

    def test_expires_is_None(self):
        b = self.Backend(expires=None, app=self.app)
        assert b.expires == self.app.conf.result_expires.total_seconds()

    def test_expires_is_timedelta(self):
        b = self.Backend(expires=timedelta(minutes=1), app=self.app)
        assert b.expires == 60

    def test_mget(self):
        assert self.b.mget(['a', 'b', 'c'])
        self.b.client.mget.assert_called_with(['a', 'b', 'c'])

    def test_set_no_expire(self):
        self.b.expires = None
        self.b.set('foo', 'bar')

    def create_task(self):
        tid = uuid()
        task = Mock(name='task-{0}'.format(tid))
        task.name = 'foobarbaz'
        self.app.tasks['foobarbaz'] = task
        task.request.chord = signature(task)
        task.request.id = tid
        task.request.chord['chord_size'] = 10
        task.request.group = 'group_id'
        return task

    @patch('celery.result.GroupResult.restore')
    def test_on_chord_part_return(self, restore):
        tasks = [self.create_task() for i in range(10)]

        for i in range(10):
            self.b.on_chord_part_return(tasks[i].request, states.SUCCESS, i)
            assert self.b.client.rpush.call_count
            self.b.client.rpush.reset_mock()
        assert self.b.client.lrange.call_count
        jkey = self.b.get_key_for_group('group_id', '.j')
        tkey = self.b.get_key_for_group('group_id', '.t')
        self.b.client.delete.assert_has_calls([call(jkey), call(tkey)])
        self.b.client.expire.assert_has_calls([
            call(jkey, 86400), call(tkey, 86400),
        ])

    def test_on_chord_part_return__success(self):
        with self.chord_context(2) as (_, request, callback):
            self.b.on_chord_part_return(request, states.SUCCESS, 10)
            callback.delay.assert_not_called()
            self.b.on_chord_part_return(request, states.SUCCESS, 20)
            callback.delay.assert_called_with([10, 20])

    def test_on_chord_part_return__callback_raises(self):
        with self.chord_context(1) as (_, request, callback):
            callback.delay.side_effect = KeyError(10)
            task = self.app._tasks['add'] = Mock(name='add_task')
            self.b.on_chord_part_return(request, states.SUCCESS, 10)
            task.backend.fail_from_current_stack.assert_called_with(
                callback.id, exc=ANY,
            )

    def test_on_chord_part_return__ChordError(self):
        with self.chord_context(1) as (_, request, callback):
            self.b.client.pipeline = ContextMock()
            raise_on_second_call(self.b.client.pipeline, ChordError())
            self.b.client.pipeline.return_value.rpush().llen().get().expire(
            ).expire().execute.return_value = (1, 1, 0, 4, 5)
            task = self.app._tasks['add'] = Mock(name='add_task')
            self.b.on_chord_part_return(request, states.SUCCESS, 10)
            task.backend.fail_from_current_stack.assert_called_with(
                callback.id, exc=ANY,
            )

    def test_on_chord_part_return__other_error(self):
        with self.chord_context(1) as (_, request, callback):
            self.b.client.pipeline = ContextMock()
            raise_on_second_call(self.b.client.pipeline, RuntimeError())
            self.b.client.pipeline.return_value.rpush().llen().get().expire(
            ).expire().execute.return_value = (1, 1, 0, 4, 5)
            task = self.app._tasks['add'] = Mock(name='add_task')
            self.b.on_chord_part_return(request, states.SUCCESS, 10)
            task.backend.fail_from_current_stack.assert_called_with(
                callback.id, exc=ANY,
            )

    @contextmanager
    def chord_context(self, size=1):
        with patch('celery.backends.redis.maybe_signature') as ms:
            tasks = [self.create_task() for i in range(size)]
            request = Mock(name='request')
            request.id = 'id1'
            request.group = 'gid1'
            callback = ms.return_value = Signature('add')
            callback.id = 'id1'
            callback['chord_size'] = size
            callback.delay = Mock(name='callback.delay')
            yield tasks, request, callback

    def test_process_cleanup(self):
        self.b.process_cleanup()

    def test_get_set_forget(self):
        tid = uuid()
        self.b.store_result(tid, 42, states.SUCCESS)
        assert self.b.get_state(tid) == states.SUCCESS
        assert self.b.get_result(tid) == 42
        self.b.forget(tid)
        assert self.b.get_state(tid) == states.PENDING

    def test_set_expires(self):
        self.b = self.Backend(expires=512, app=self.app)
        tid = uuid()
        key = self.b.get_key_for_task(tid)
        self.b.store_result(tid, 42, states.SUCCESS)
        self.b.client.expire.assert_called_with(
            key, 512,
        )


class test_SentinelBackend:
    def get_backend(self):
        from celery.backends.redis import SentinelBackend

        class _SentinelBackend(SentinelBackend):
            redis = redis
            sentinel = sentinel

        return _SentinelBackend

    def get_E_LOST(self):
        from celery.backends.redis import E_LOST
        return E_LOST

    def setup(self):
        self.Backend = self.get_backend()
        self.E_LOST = self.get_E_LOST()
        self.b = self.Backend(app=self.app)

    @pytest.mark.usefixtures('depends_on_current_app')
    @skip.unless_module('redis')
    def test_reduce(self):
        from celery.backends.redis import SentinelBackend
        x = SentinelBackend(app=self.app)
        assert loads(dumps(x))

    def test_no_redis(self):
        self.Backend.redis = None
        with pytest.raises(ImproperlyConfigured):
            self.Backend(app=self.app)

    def test_url(self):
        self.app.conf.redis_socket_timeout = 30.0
        self.app.conf.redis_socket_connect_timeout = 100.0
        x = self.Backend(
            'sentinel://:test@github.com:123/1;'
            'sentinel://:test@github.com:124/1',
            app=self.app,
        )
        assert x.connparams
        assert "host" not in x.connparams
        assert x.connparams['db'] == 1
        assert "port" not in x.connparams
        assert x.connparams['password'] == "test"
        assert len(x.connparams['hosts']) == 2
        expected_hosts = ["github.com", "github.com"]
        found_hosts = [cp['host'] for cp in x.connparams['hosts']]
        assert found_hosts == expected_hosts

        expected_ports = [123, 124]
        found_ports = [cp['port'] for cp in x.connparams['hosts']]
        assert found_ports == expected_ports

        expected_passwords = ["test", "test"]
        found_passwords = [cp['password'] for cp in x.connparams['hosts']]
        assert found_passwords == expected_passwords

        expected_dbs = [1, 1]
        found_dbs = [cp['db'] for cp in x.connparams['hosts']]
        assert found_dbs == expected_dbs

    def test_get_sentinel_instance(self):
        x = self.Backend(
            'sentinel://:test@github.com:123/1;'
            'sentinel://:test@github.com:124/1',
            app=self.app,
        )
        sentinel_instance = x._get_sentinel_instance(**x.connparams)
        assert sentinel_instance.sentinel_kwargs == {}
        assert sentinel_instance.connection_kwargs['db'] == 1
        assert sentinel_instance.connection_kwargs['password'] == "test"
        assert len(sentinel_instance.sentinels) == 2

    def test_get_pool(self):
        x = self.Backend(
            'sentinel://:test@github.com:123/1;'
            'sentinel://:test@github.com:124/1',
            app=self.app,
        )
        pool = x._get_pool(**x.connparams)
        assert pool
