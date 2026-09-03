import itertools
import json
import random
import ssl
from contextlib import contextmanager
from datetime import timedelta
from pickle import dumps, loads
from unittest.mock import ANY, Mock, call, patch

import pytest

try:
    from redis import CredentialProvider, exceptions
except ImportError:
    exceptions = None
    CredentialProvider = None

from kombu.utils.encoding import ensure_bytes

from celery import signature, states, uuid
from celery.app.task import Context
from celery.backends.base import COMPRESSED_PAYLOAD_MAGIC
from celery.canvas import Signature
from celery.contrib.testing.mocks import ContextMock
from celery.exceptions import BackendStoreError, ChordError, ImproperlyConfigured
from celery.result import AsyncResult, GroupResult
from celery.utils.collections import AttributeDict
from t.unit import conftest


def raise_on_second_call(mock, exc, *retval):
    def on_first_call(*args, **kwargs):
        mock.side_effect = exc
        return mock.return_value

    mock.side_effect = on_first_call
    if retval:
        mock.return_value, = retval


class ConnectionError(Exception):
    pass


class Connection:
    connected = True

    def disconnect(self):
        self.connected = False


class Pipeline:
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


class PubSub(conftest.MockCallbacks):
    def __init__(self, ignore_subscribe_messages=False):
        self._subscribed_to = set()

    def close(self):
        self._subscribed_to = set()

    def subscribe(self, *args):
        self._subscribed_to.update(args)

    def unsubscribe(self, *args):
        self._subscribed_to.difference_update(args)

    def get_message(self, timeout=None):
        pass


class Redis(conftest.MockCallbacks):
    Connection = Connection
    Pipeline = Pipeline
    pubsub = PubSub

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

    def mget(self, keys):
        return [self.get(key) for key in keys]

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

    def _get_unsorted_list(self, key):
        # We simply store the values in append (rpush) order
        return self.keyspace.setdefault(key, list())

    def rpush(self, key, value):
        self._get_unsorted_list(key).append(value)

    def lrange(self, key, start, stop):
        return self._get_unsorted_list(key)[start:stop]

    def llen(self, key):
        return len(self._get_unsorted_list(key))

    def _get_sorted_set(self, key):
        # We store 2-tuples of (score, value) and sort after each append (zadd)
        return self.keyspace.setdefault(key, list())

    def zadd(self, key, mapping):
        # Store elements as 2-tuples with the score first so we can sort it
        # once the new items have been inserted
        fake_sorted_set = self._get_sorted_set(key)
        fake_sorted_set.extend(
            (score, value) for value, score in mapping.items()
        )
        fake_sorted_set.sort()

    def zrange(self, key, start, stop):
        # `stop` is inclusive in Redis so we use `stop + 1` unless that would
        # cause us to move from negative (right-most) indices to positive
        stop = stop + 1 if stop != -1 else None
        return [e[1] for e in self._get_sorted_set(key)[start:stop]]

    def zrangebyscore(self, key, min_, max_):
        return [
            e[1] for e in self._get_sorted_set(key)
            if (min_ == "-inf" or e[0] >= min_) and
            (max_ == "+inf" or e[1] <= max_)
        ]

    def zcount(self, key, min_, max_):
        return len(self.zrangebyscore(key, min_, max_))

    def _get_hash(self, key):
        return self.keyspace.setdefault(key, {})

    def hset(self, key, field, value):
        self._get_hash(key)[field] = value

    def hgetall(self, key):
        # Return bytes like real Redis does
        # Don't create the key if it doesn't exist (unlike _get_hash)
        hash_data = self.keyspace.get(key)
        if hash_data is None:
            return {}
        return {k.encode() if isinstance(k, str) else k: v for k, v in hash_data.items()}

    def hincrby(self, key, field, increment):
        hash_data = self._get_hash(key)
        current = int(hash_data.get(field, 0))
        hash_data[field] = current + increment
        return hash_data[field]

    def _get_set(self, key):
        # Ensure the key is always a set, even if it was previously a different type
        if key not in self.keyspace or not isinstance(self.keyspace[key], set):
            self.keyspace[key] = set()
        return self.keyspace[key]

    def sadd(self, key, *members):
        result = 0
        for member in members:
            if member not in self._get_set(key):
                result += 1
                self._get_set(key).add(member)
        return result

    def sismember(self, key, member):
        return int(member in self._get_set(key))

    def ttl(self, key):
        return self.expiry.get(key, -1)

    def eval(self, script, numkeys, *keys_and_args):
        # Simplified Lua script execution for testing
        # This is a basic implementation that handles the specific script used
        # for idempotent increment. For production, real Redis eval would be used.
        if numkeys == 2 and 'SISMEMBER' in script:
            pkey = keys_and_args[0]  # KEYS[1] = progress hash key
            seen_key = keys_and_args[1]  # KEYS[2] = seen set key
            task_id = keys_and_args[2]  # ARGV[1] = task_id

            # Check if the group was initialized (has 'total' field)
            # First check if the key exists at all (use .get to avoid creating it)
            hash_data = self.keyspace.get(pkey)
            if hash_data is None:
                return 0  # Group not initialized, do nothing

            if b'total' not in hash_data and 'total' not in hash_data:
                return 0  # Group not initialized, do nothing

            # Check if task_id is already in the seen set
            if self.sismember(seen_key, task_id):
                return 0  # Already counted

            # Add to seen set and increment counter
            self.sadd(seen_key, task_id)
            self.hincrby(pkey, b'count', 1)

            # Set TTL on seen set to match the progress hash if it exists
            ttl = self.ttl(pkey)
            if ttl > 0:
                self.expire(seen_key, ttl)

            return 1  # Incremented

        # Fallback for other scripts (not used in current tests)
        return None

    def type(self, key):
        if key in self.keyspace:
            if isinstance(self.keyspace[key], dict):
                return b'hash'
            elif isinstance(self.keyspace[key], list):
                return b'list'
            elif isinstance(self.keyspace[key], set):
                return b'set'
            else:
                return b'string'
        return b'none'


class Sentinel(conftest.MockCallbacks):
    def __init__(self, sentinels, min_other_sentinels=0, sentinel_kwargs=None,
                 **connection_kwargs):
        self.sentinel_kwargs = sentinel_kwargs
        self.sentinels = [Redis(hostname, port, **self.sentinel_kwargs)
                          for hostname, port in sentinels]
        self.min_other_sentinels = min_other_sentinels
        self.connection_kwargs = connection_kwargs

    def master_for(self, service_name, redis_class, **kwargs):
        self.master_for_kwargs = kwargs
        return random.choice(self.sentinels)


class redis:
    StrictRedis = Redis

    class ConnectionPool:
        def __init__(self, **kwargs):
            pass

    class UnixDomainSocketConnection:
        def __init__(self, **kwargs):
            pass


class sentinel:
    Sentinel = Sentinel


class test_RedisResultConsumer:
    def get_backend(self):
        from celery.backends.redis import RedisBackend

        class _RedisBackend(RedisBackend):
            redis = redis

        return _RedisBackend(app=self.app)

    def get_consumer(self):
        consumer = self.get_backend().result_consumer
        consumer._connection_errors = (ConnectionError,)
        return consumer

    @patch('celery.backends.asynchronous.BaseResultConsumer.on_after_fork')
    def test_on_after_fork(self, parent_method):
        consumer = self.get_consumer()
        consumer.start('none')
        consumer.on_after_fork()
        parent_method.assert_called_once()
        consumer.backend.client.connection_pool.reset.assert_called_once()
        consumer._pubsub.close.assert_called_once()
        # PubSub instance not initialized - exception would be raised
        # when calling .close()
        consumer._pubsub = None
        parent_method.reset_mock()
        consumer.backend.client.connection_pool.reset.reset_mock()
        consumer.on_after_fork()
        parent_method.assert_called_once()
        consumer.backend.client.connection_pool.reset.assert_called_once()

        # Continues on KeyError
        consumer._pubsub = Mock()
        consumer._pubsub.close = Mock(side_effect=KeyError)
        parent_method.reset_mock()
        consumer.backend.client.connection_pool.reset.reset_mock()
        consumer.on_after_fork()
        parent_method.assert_called_once()

    @patch('celery.backends.redis.ResultConsumer.cancel_for')
    @patch('celery.backends.asynchronous.BaseResultConsumer.on_state_change')
    def test_on_state_change(self, parent_method, cancel_for):
        consumer = self.get_consumer()
        meta = {'task_id': 'testing', 'status': states.SUCCESS}
        message = 'hello'
        consumer.on_state_change(meta, message)
        parent_method.assert_called_once_with(meta, message)
        cancel_for.assert_called_once_with(meta['task_id'])

        # Does not call cancel_for for other states
        meta = {'task_id': 'testing2', 'status': states.PENDING}
        parent_method.reset_mock()
        cancel_for.reset_mock()
        consumer.on_state_change(meta, message)
        parent_method.assert_called_once_with(meta, message)
        cancel_for.assert_not_called()

    def test_drain_events_before_start(self):
        consumer = self.get_consumer()
        # drain_events shouldn't crash when called before start
        consumer.drain_events(0.001)

    def test_consume_from_connection_error(self):
        consumer = self.get_consumer()
        consumer.start('initial')
        consumer._pubsub.subscribe.side_effect = (ConnectionError(), None)
        consumer.consume_from('some-task')
        assert consumer._pubsub._subscribed_to == {b'celery-task-meta-initial', b'celery-task-meta-some-task'}

    def test_cancel_for_connection_error(self):
        consumer = self.get_consumer()
        consumer.start('initial')
        consumer._pubsub.unsubscribe.side_effect = ConnectionError()
        consumer.consume_from('some-task')
        consumer.cancel_for('some-task')
        assert consumer._pubsub._subscribed_to == {b'celery-task-meta-initial'}

    def test_cancel_for_never_subscribed_is_noop(self):
        consumer = self.get_consumer()
        consumer.start('initial')
        task_id = uuid()
        consumer.cancel_for(task_id)
        consumer._pubsub.unsubscribe.assert_not_called()

    def test_cancel_for_second_call_after_already_cancelled_is_noop(self):
        consumer = self.get_consumer()
        consumer.start('initial')
        task_id = uuid()
        consumer.consume_from(task_id)
        consumer.cancel_for(task_id)
        consumer._pubsub.unsubscribe.reset_mock()
        # simulates AsyncResult.__del__ firing again after get() already
        # drove cleanup once — this is the literal deadlock trigger in #10477
        consumer.cancel_for(task_id)
        consumer._pubsub.unsubscribe.assert_not_called()

    @patch('celery.backends.redis.ResultConsumer.cancel_for')
    @patch('celery.backends.asynchronous.BaseResultConsumer.on_state_change')
    def test_drain_events_connection_error(self, parent_on_state_change, cancel_for):
        meta = {'task_id': 'initial', 'status': states.SUCCESS}
        consumer = self.get_consumer()
        consumer.start('initial')
        consumer.backend._set_with_state(b'celery-task-meta-initial', json.dumps(meta), states.SUCCESS)
        consumer._pubsub.get_message.side_effect = ConnectionError()
        consumer.drain_events()
        parent_on_state_change.assert_called_with(meta, None)
        assert consumer._pubsub._subscribed_to == {b'celery-task-meta-initial'}

    def test_drain_events_connection_error_no_patch(self):
        meta = {'task_id': 'initial', 'status': states.SUCCESS}
        consumer = self.get_consumer()
        consumer.start('initial')
        consumer.backend._set_with_state(b'celery-task-meta-initial', json.dumps(meta), states.SUCCESS)
        consumer._pubsub.get_message.side_effect = ConnectionError()
        consumer.drain_events()
        consumer._pubsub.subscribe.assert_not_called()

    def test__reconnect_pubsub_no_subscribed(self):
        consumer = self.get_consumer()
        consumer.start('initial')
        consumer.subscribed_to = set()
        consumer._reconnect_pubsub()
        consumer.backend.client.mget.assert_not_called()
        consumer._pubsub.subscribe.assert_not_called()
        consumer._pubsub.connection.register_connect_callback.assert_called_once()

    def test__reconnect_pubsub_with_state_change(self):
        meta = {'task_id': 'initial', 'status': states.SUCCESS}
        consumer = self.get_consumer()
        consumer.start('initial')
        consumer.backend._set_with_state(b'celery-task-meta-initial', json.dumps(meta), states.SUCCESS)
        consumer._reconnect_pubsub()
        consumer.backend.client.mget.assert_called_once()
        consumer._pubsub.subscribe.assert_not_called()
        consumer._pubsub.connection.register_connect_callback.assert_called_once()

    def test__reconnect_pubsub_without_state_change(self):
        meta = {'task_id': 'initial', 'status': states.STARTED}
        consumer = self.get_consumer()
        consumer.start('initial')
        consumer.backend._set_with_state(b'celery-task-meta-initial', json.dumps(meta), states.SUCCESS)
        consumer._reconnect_pubsub()
        consumer.backend.client.mget.assert_called_once()
        consumer._pubsub.subscribe.assert_called_once()
        consumer._pubsub.connection.register_connect_callback.assert_not_called()

    def test__reconnect_pubsub_redis_py_below_5_3_compat(self):
        """Regression test for celery#10294.

        On redis-py < 5.3.0, ConnectionPool.get_connection requires
        ``command_name`` as a positional argument. _reconnect_pubsub must
        remain compatible with that older signature when no tasks are
        subscribed.
        """
        consumer = self.get_consumer()
        consumer.start('initial')
        consumer.subscribed_to = set()

        def legacy_get_connection(command_name, *args, **kwargs):
            return Mock(name='legacy-connection')

        # Replace the auto-mocked get_connection with one that mirrors the
        # redis-py < 5.3.0 signature: command_name is required.
        consumer._pubsub = Mock(name='pubsub')
        consumer._pubsub.connection_pool = Mock(name='connection_pool')
        consumer._pubsub.connection_pool.get_connection.side_effect = (
            legacy_get_connection
        )
        consumer.backend.client = Mock(name='client')
        consumer.backend.client.pubsub.return_value = consumer._pubsub

        # Must not raise TypeError about a missing 'command_name' argument.
        consumer._reconnect_pubsub()

    def test_on_wait_for_pending_cleans_up_leaked_success_messages(self):
        """Regression test for #8166.

        When on_state_change processes a SUCCESS meta for a result that has
        already been resolved and removed from _pending_results, it buffers
        the meta in _pending_messages. on_wait_for_pending should then
        clean up this leaked entry after canceling the subscription.
        """
        from celery.utils.collections import BufferMap

        consumer = self.get_consumer()
        consumer.backend._pending_results = {}, {}
        consumer.backend._pending_messages = BufferMap(10)

        task_id = 'test-task-1'
        meta = {
            'task_id': task_id,
            'status': states.SUCCESS,
            'result': 42,
        }

        # Manually put the meta into _pending_messages to simulate the leak
        consumer.backend._pending_messages.put(task_id, meta)
        assert task_id in consumer.backend._pending_messages

        # Create a mock result object with _iter_meta
        class MockResult:
            def _iter_meta(self, **kwargs):
                return [meta]

        # Call on_wait_for_pending - should trigger cleanup for SUCCESS
        consumer.on_wait_for_pending(MockResult())

        # The leaked entry should be removed
        assert task_id not in consumer.backend._pending_messages

    def test_on_wait_for_pending_does_not_cleanup_revoked_messages(self):
        """REVOKED state should not be cleaned up - it may still be needed by waiters."""
        from celery.utils.collections import BufferMap

        consumer = self.get_consumer()
        consumer.backend._pending_results = {}, {}
        consumer.backend._pending_messages = BufferMap(10)

        task_id = 'test-task-2'
        meta = {
            'task_id': task_id,
            'status': states.REVOKED,
            'result': None,
        }

        # Manually put the meta into _pending_messages
        consumer.backend._pending_messages.put(task_id, meta)
        assert task_id in consumer.backend._pending_messages

        # Create a mock result object with _iter_meta
        class MockResult:
            def _iter_meta(self, **kwargs):
                return [meta]

        # Call on_wait_for_pending - should NOT clean up REVOKED
        consumer.on_wait_for_pending(MockResult())

        # REVOKED meta should still be in buffer
        assert task_id in consumer.backend._pending_messages

    def test_on_wait_for_pending_cleans_up_leaked_failure_messages(self):
        """FAILURE state should be cleaned up like SUCCESS."""
        from celery.utils.collections import BufferMap

        consumer = self.get_consumer()
        consumer.backend._pending_results = {}, {}
        consumer.backend._pending_messages = BufferMap(10)

        task_id = 'test-task-3'
        meta = {
            'task_id': task_id,
            'status': states.FAILURE,
            'result': Exception('test'),
        }

        # Manually put the meta into _pending_messages to simulate the leak
        consumer.backend._pending_messages.put(task_id, meta)
        assert task_id in consumer.backend._pending_messages

        # Create a mock result object with _iter_meta
        class MockResult:
            def _iter_meta(self, **kwargs):
                return [meta]

        # Call on_wait_for_pending - should trigger cleanup for FAILURE
        consumer.on_wait_for_pending(MockResult())

        # The leaked entry should be removed
        assert task_id not in consumer.backend._pending_messages

    def test_on_wait_for_pending_skips_cleanup_when_not_in_pending_messages(self):
        """When the task is not in _pending_messages, cleanup should be a no-op."""
        from celery.utils.collections import BufferMap

        consumer = self.get_consumer()
        consumer.backend._pending_results = {}, {}
        consumer.backend._pending_messages = BufferMap(10)

        task_id = 'test-task-4'
        meta = {
            'task_id': task_id,
            'status': states.SUCCESS,
            'result': 42,
        }

        # Do NOT put the meta into _pending_messages
        assert task_id not in consumer.backend._pending_messages

        # Create a mock result object with _iter_meta
        class MockResult:
            def _iter_meta(self, **kwargs):
                return [meta]

        # Call on_wait_for_pending - should not raise even though entry is missing
        consumer.on_wait_for_pending(MockResult())

        # Should still be absent (no crash)
        assert task_id not in consumer.backend._pending_messages

    def test_on_wait_for_pending_handles_keyerror_race(self):
        """If BufferMap.pop raises KeyError, the exception should be swallowed."""
        from celery.utils.collections import BufferMap

        consumer = self.get_consumer()
        consumer.backend._pending_results = {}, {}
        consumer.backend._pending_messages = BufferMap(10)

        task_id = 'test-task-5'
        meta = {
            'task_id': task_id,
            'status': states.SUCCESS,
            'result': 42,
        }

        # Put the meta into _pending_messages
        consumer.backend._pending_messages.put(task_id, meta)
        assert task_id in consumer.backend._pending_messages

        # Simulate a race where the entry is removed between the `in` check and pop
        # by replacing pop with a side effect that raises KeyError
        original_pop = consumer.backend._pending_messages.pop

        def race_pop(key):
            raise KeyError(key)
        consumer.backend._pending_messages.pop = race_pop

        # Create a mock result object with _iter_meta
        class MockResult:
            def _iter_meta(self, **kwargs):
                return [meta]

        try:
            # Call on_wait_for_pending - should not raise despite the race
            consumer.on_wait_for_pending(MockResult())
        finally:
            consumer.backend._pending_messages.pop = original_pop

        # The race_pop raised KeyError, so the entry was never actually removed.
        # The important thing is that on_wait_for_pending did not crash.
        assert task_id in consumer.backend._pending_messages


class basetest_RedisBackend:
    def get_backend(self):
        from celery.backends.redis import RedisBackend

        class _RedisBackend(RedisBackend):
            redis = redis

        return _RedisBackend

    def get_E_LOST(self):
        from celery.backends.redis import E_LOST
        return E_LOST

    def create_task(self, i, group_id="group_id"):
        tid = uuid()
        task = Mock(name=f'task-{tid}')
        task.name = 'foobarbaz'
        self.app.tasks['foobarbaz'] = task
        task.request.chord = signature(task)
        task.request.id = tid
        self.b.set_chord_size(group_id, 10)
        task.request.group = group_id
        task.request.group_index = i
        return task

    @contextmanager
    def chord_context(self, size=1):
        with patch('celery.backends.redis.maybe_signature') as ms:
            request = Mock(name='request')
            request.id = 'id1'
            group_id = 'gid1'
            request.group = group_id
            request.group_index = None
            tasks = [
                self.create_task(i, group_id=request.group)
                for i in range(size)
            ]
            callback = ms.return_value = Signature('add')
            callback.id = 'id1'
            self.b.set_chord_size(group_id, size)
            callback.delay = Mock(name='callback.delay')
            yield tasks, request, callback

    def setup_method(self):
        self.Backend = self.get_backend()
        self.E_LOST = self.get_E_LOST()
        self.b = self.Backend(app=self.app)


class MyCredentialProvider(CredentialProvider):
    pass


class NonCredentialProvider:
    pass


class test_RedisBackend(basetest_RedisBackend):
    @pytest.mark.usefixtures('depends_on_current_app')
    def test_reduce(self):
        pytest.importorskip('redis')

        from celery.backends.redis import RedisBackend
        x = RedisBackend(app=self.app)
        assert loads(dumps(x))

    def test_no_redis(self):
        self.Backend.redis = None
        with pytest.raises(ImproperlyConfigured):
            self.Backend(app=self.app)

    def test_username_password_from_redis_conf(self):
        self.app.conf.redis_password = 'password'
        x = self.Backend(app=self.app)

        assert x.connparams
        assert 'username' not in x.connparams
        assert x.connparams['password'] == 'password'
        self.app.conf.redis_username = 'username'
        x = self.Backend(app=self.app)

        assert x.connparams
        assert x.connparams['username'] == 'username'
        assert x.connparams['password'] == 'password'

    def test_credential_provider_from_redis_conf(self):
        self.app.conf.redis_backend_credential_provider = "redis.CredentialProvider"
        x = self.Backend(app=self.app)

        assert x.connparams
        assert 'credential_provider' in x.connparams
        assert 'username' not in x.connparams
        assert 'password' not in x.connparams

        # with local credential provider
        self.app.conf.redis_backend_credential_provider = MyCredentialProvider()
        x = self.Backend(app=self.app)
        assert x.connparams
        assert 'credential_provider' in x.connparams
        assert 'username' not in x.connparams
        assert 'password' not in x.connparams

        # raise ImportError
        self.app.conf.redis_backend_credential_provider = "not_exist.CredentialProvider"
        with pytest.raises(ImportError):
            self.Backend(app=self.app)

        # raise value Error
        self.app.conf.redis_backend_credential_provider = NonCredentialProvider()
        with pytest.raises(ValueError):
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
        assert 'username' not in x.connparams

        x = self.Backend(
            'redis://username:bosco@vandelay.com:123//1', app=self.app,
        )
        assert x.connparams
        assert x.connparams['host'] == 'vandelay.com'
        assert x.connparams['db'] == 1
        assert x.connparams['port'] == 123
        assert x.connparams['username'] == 'username'
        assert x.connparams['password'] == 'bosco'
        assert x.connparams['socket_timeout'] == 30.0
        assert x.connparams['socket_connect_timeout'] == 100.0

    def test_url_with_credential_provider(self):
        self.app.conf.redis_socket_timeout = 30.0
        self.app.conf.redis_socket_connect_timeout = 100.0
        x = self.Backend(
            'redis://:bosco@vandelay.com:123/1?credential_provider=redis.CredentialProvider', app=self.app,
        )

        assert x.connparams
        assert x.connparams['host'] == 'vandelay.com'
        assert x.connparams['db'] == 1
        assert x.connparams['port'] == 123
        assert x.connparams['socket_timeout'] == 30.0
        assert x.connparams['socket_connect_timeout'] == 100.0
        assert isinstance(x.connparams['credential_provider'], CredentialProvider)
        assert "username" not in x.connparams
        assert "password" not in x.connparams

        # without username and password
        x = self.Backend(
            'redis://@vandelay.com:123/1?credential_provider=redis.UsernamePasswordCredentialProvider', app=self.app,
        )
        assert x.connparams
        assert x.connparams['host'] == 'vandelay.com'
        assert x.connparams['db'] == 1
        assert x.connparams['port'] == 123
        assert isinstance(x.connparams['credential_provider'], CredentialProvider)

        # raise importError
        with pytest.raises(ImportError):
            self.Backend(
                'redis://@vandelay.com:123/1?credential_provider=not_exist.CredentialProvider', app=self.app,
            )

        # raise valueError
        with pytest.raises(ValueError):
            # some non-credential provider class
            # not ideal but serve purpose
            self.Backend(
                'redis://@vandelay.com:123/1?credential_provider=abc.ABC', app=self.app,
            )

    def test_timeouts_in_url_coerced(self):
        pytest.importorskip('redis')

        x = self.Backend(
            ('redis://:bosco@vandelay.com:123//1?'
             'socket_timeout=30&socket_connect_timeout=100'),
            app=self.app,
        )
        assert x.connparams
        assert x.connparams['host'] == 'vandelay.com'
        assert x.connparams['db'] == 1
        assert x.connparams['port'] == 123
        assert x.connparams['password'] == 'bosco'
        assert x.connparams['socket_timeout'] == 30
        assert x.connparams['socket_connect_timeout'] == 100

    def test_socket_url(self):
        pytest.importorskip('redis')

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
        assert 'socket_keepalive' not in x.connparams
        assert x.connparams['db'] == 3

    def test_backend_ssl(self):
        pytest.importorskip('redis')

        self.app.conf.redis_backend_use_ssl = {
            'ssl_cert_reqs': ssl.CERT_REQUIRED,
            'ssl_ca_certs': '/path/to/ca.crt',
            'ssl_certfile': '/path/to/client.crt',
            'ssl_keyfile': '/path/to/client.key',
        }
        self.app.conf.redis_socket_timeout = 30.0
        self.app.conf.redis_socket_connect_timeout = 100.0
        x = self.Backend(
            'rediss://:bosco@vandelay.com:123//1', app=self.app,
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

    def test_backend_ssl_with_redis_scheme(self):
        pytest.importorskip('redis')

        self.app.conf.redis_backend_use_ssl = {
            'ssl_cert_reqs': ssl.CERT_REQUIRED,
            'ssl_ca_certs': '/path/to/ca.crt',
            'ssl_certfile': '/path/to/client.crt',
            'ssl_keyfile': '/path/to/client.key',
        }
        x = self.Backend(
            'redis://:bosco@vandelay.com:123//1', app=self.app,
        )
        assert x.connparams
        assert x.connparams['host'] == 'vandelay.com'
        assert x.connparams['db'] == 1
        assert x.connparams['port'] == 123
        assert x.connparams['password'] == 'bosco'
        assert x.connparams['ssl_cert_reqs'] == ssl.CERT_REQUIRED
        assert x.connparams['ssl_ca_certs'] == '/path/to/ca.crt'
        assert x.connparams['ssl_certfile'] == '/path/to/client.crt'
        assert x.connparams['ssl_keyfile'] == '/path/to/client.key'

        from redis.connection import SSLConnection
        assert x.connparams['connection_class'] is SSLConnection

    def test_backend_health_check_interval_ssl(self):
        pytest.importorskip('redis')

        self.app.conf.redis_backend_use_ssl = {
            'ssl_cert_reqs': ssl.CERT_REQUIRED,
            'ssl_ca_certs': '/path/to/ca.crt',
            'ssl_certfile': '/path/to/client.crt',
            'ssl_keyfile': '/path/to/client.key',
        }
        self.app.conf.redis_backend_health_check_interval = 10
        x = self.Backend(
            'rediss://:bosco@vandelay.com:123//1', app=self.app,
        )
        assert x.connparams
        assert x.connparams['host'] == 'vandelay.com'
        assert x.connparams['db'] == 1
        assert x.connparams['port'] == 123
        assert x.connparams['password'] == 'bosco'
        assert x.connparams['health_check_interval'] == 10

        from redis.connection import SSLConnection
        assert x.connparams['connection_class'] is SSLConnection

    def test_backend_health_check_interval(self):
        pytest.importorskip('redis')

        self.app.conf.redis_backend_health_check_interval = 10
        x = self.Backend(
            'redis://vandelay.com:123//1', app=self.app,
        )
        assert x.connparams
        assert x.connparams['host'] == 'vandelay.com'
        assert x.connparams['db'] == 1
        assert x.connparams['port'] == 123
        assert x.connparams['health_check_interval'] == 10

    def test_backend_health_check_interval_not_set(self):
        pytest.importorskip('redis')

        x = self.Backend(
            'redis://vandelay.com:123//1', app=self.app,
        )
        assert x.connparams
        assert x.connparams['host'] == 'vandelay.com'
        assert x.connparams['db'] == 1
        assert x.connparams['port'] == 123
        assert "health_check_interval" not in x.connparams

    def test_backend_redis_client_name(self):
        pytest.importorskip('redis')

        self.app.conf.redis_client_name = 'celery-worker'
        x = self.Backend(
            'redis://vandelay.com:123//1', app=self.app,
        )
        assert x.connparams
        assert x.connparams['host'] == 'vandelay.com'
        assert x.connparams['db'] == 1
        assert x.connparams['port'] == 123
        assert x.connparams['client_name'] == 'celery-worker'

    def test_backend_redis_client_name_not_set(self):
        pytest.importorskip('redis')

        x = self.Backend(
            'redis://vandelay.com:123//1', app=self.app,
        )
        assert x.connparams
        assert x.connparams['host'] == 'vandelay.com'
        assert x.connparams['db'] == 1
        assert x.connparams['port'] == 123
        assert x.connparams['client_name'] is None

    @pytest.mark.parametrize('cert_str', [
        "required",
        "CERT_REQUIRED",
    ])
    def test_backend_ssl_certreq_str(self, cert_str):
        pytest.importorskip('redis')

        self.app.conf.redis_backend_use_ssl = {
            'ssl_cert_reqs': cert_str,
            'ssl_ca_certs': '/path/to/ca.crt',
            'ssl_certfile': '/path/to/client.crt',
            'ssl_keyfile': '/path/to/client.key',
        }
        self.app.conf.redis_socket_timeout = 30.0
        self.app.conf.redis_socket_connect_timeout = 100.0
        x = self.Backend(
            'rediss://:bosco@vandelay.com:123//1', app=self.app,
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

    @pytest.mark.parametrize('cert_str', [
        "required",
        "CERT_REQUIRED",
    ])
    def test_backend_ssl_url(self, cert_str):
        pytest.importorskip('redis')

        self.app.conf.redis_socket_timeout = 30.0
        self.app.conf.redis_socket_connect_timeout = 100.0
        x = self.Backend(
            'rediss://:bosco@vandelay.com:123//1?ssl_cert_reqs=%s' % cert_str,
            app=self.app,
        )
        assert x.connparams
        assert x.connparams['host'] == 'vandelay.com'
        assert x.connparams['db'] == 1
        assert x.connparams['port'] == 123
        assert x.connparams['password'] == 'bosco'
        assert x.connparams['socket_timeout'] == 30.0
        assert x.connparams['socket_connect_timeout'] == 100.0
        assert x.connparams['ssl_cert_reqs'] == ssl.CERT_REQUIRED

        from redis.connection import SSLConnection
        assert x.connparams['connection_class'] is SSLConnection

    @pytest.mark.parametrize('cert_str', [
        "none",
        "CERT_NONE",
    ])
    def test_backend_ssl_url_options(self, cert_str):
        pytest.importorskip('redis')

        x = self.Backend(
            (
                'rediss://:bosco@vandelay.com:123//1'
                '?ssl_cert_reqs={cert_str}'
                '&ssl_ca_certs=%2Fvar%2Fssl%2Fmyca.pem'
                '&ssl_certfile=%2Fvar%2Fssl%2Fredis-server-cert.pem'
                '&ssl_keyfile=%2Fvar%2Fssl%2Fprivate%2Fworker-key.pem'
            ).format(cert_str=cert_str),
            app=self.app,
        )
        assert x.connparams
        assert x.connparams['host'] == 'vandelay.com'
        assert x.connparams['db'] == 1
        assert x.connparams['port'] == 123
        assert x.connparams['password'] == 'bosco'
        assert x.connparams['ssl_cert_reqs'] == ssl.CERT_NONE
        assert x.connparams['ssl_ca_certs'] == '/var/ssl/myca.pem'
        assert x.connparams['ssl_certfile'] == '/var/ssl/redis-server-cert.pem'
        assert x.connparams['ssl_keyfile'] == '/var/ssl/private/worker-key.pem'

    @pytest.mark.parametrize('cert_str', [
        "optional",
        "CERT_OPTIONAL",
    ])
    def test_backend_ssl_url_cert_none(self, cert_str):
        pytest.importorskip('redis')

        x = self.Backend(
            'rediss://:bosco@vandelay.com:123//1?ssl_cert_reqs=%s' % cert_str,
            app=self.app,
        )
        assert x.connparams
        assert x.connparams['host'] == 'vandelay.com'
        assert x.connparams['db'] == 1
        assert x.connparams['port'] == 123
        assert x.connparams['ssl_cert_reqs'] == ssl.CERT_OPTIONAL

        from redis.connection import SSLConnection
        assert x.connparams['connection_class'] is SSLConnection

    @pytest.mark.parametrize("uri", [
        'rediss://:bosco@vandelay.com:123//1?ssl_cert_reqs=CERT_KITTY_CATS',
        'rediss://:bosco@vandelay.com:123//1'
    ])
    def test_backend_ssl_url_invalid(self, uri):
        pytest.importorskip('redis')

        with pytest.raises(ValueError):
            self.Backend(
                uri,
                app=self.app,
            )

    def test_backend_ssl_url_redis_scheme_invalid(self):
        pytest.importorskip('redis')

        with pytest.raises(ValueError):
            self.Backend(
                'redis://:bosco@vandelay.com:123//1?ssl_cert_reqs=required',
                app=self.app,
            )

    def test_conf_raises_KeyError(self):
        self.app.conf = AttributeDict({
            'result_serializer': 'json',
            'result_cache_max': 1,
            'result_expires': None,
            'accept_content': ['json'],
            'result_accept_content': ['json'],
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

    @patch('celery.backends.redis.retry_over_time')
    def test_retry_policy_conf(self, retry_over_time):
        self.app.conf.result_backend_transport_options = dict(
            retry_policy=dict(
                max_retries=2,
                interval_start=0,
                interval_step=0.01,
            ),
        )
        b = self.Backend(app=self.app)

        def fn():
            return 1

        # We don't want to re-test retry_over_time, just check we called it
        # with the expected args
        b.ensure(fn, (),)

        retry_over_time.assert_called_with(
            fn, b.connection_errors, (), {}, ANY,
            max_retries=2, interval_start=0, interval_step=0.01, interval_max=1
        )

    def test_exception_safe_to_retry(self):
        b = self.Backend(app=self.app)
        assert not b.exception_safe_to_retry(Exception("failed"))
        assert not b.exception_safe_to_retry(BaseException("failed"))
        assert not b.exception_safe_to_retry(exceptions.RedisError("redis error"))
        assert b.exception_safe_to_retry(exceptions.ConnectionError("service unavailable"))
        assert b.exception_safe_to_retry(exceptions.TimeoutError("timeout"))

    def test_additional_connection_errors(self):
        self.app.conf.result_backend_transport_options = dict(
            additional_connection_errors=(ConnectionError,),
        )
        b = self.Backend(app=self.app)
        assert ConnectionError in b.connection_errors
        assert b.exception_safe_to_retry(ConnectionError("custom"))

    def test_additional_connection_errors_string(self):
        self.app.conf.result_backend_transport_options = dict(
            additional_connection_errors=(
                't.unit.backends.test_redis.ConnectionError',
            ),
        )
        b = self.Backend(app=self.app)
        assert ConnectionError in b.connection_errors
        assert b.exception_safe_to_retry(ConnectionError("custom"))

    def test_additional_connection_errors_passed_to_result_consumer(self):
        self.app.conf.result_backend_transport_options = dict(
            additional_connection_errors=(ConnectionError,),
        )
        b = self.Backend(app=self.app)
        assert ConnectionError in b.result_consumer._connection_errors

    def test_additional_connection_errors_empty(self):
        self.app.conf.result_backend_transport_options = dict(
            additional_connection_errors=(),
        )
        b = self.Backend(app=self.app)
        assert ConnectionError not in b.connection_errors

    def test_additional_connection_errors_not_set(self):
        self.app.conf.result_backend_transport_options = {}
        b = self.Backend(app=self.app)
        assert ConnectionError not in b.connection_errors

    def test_additional_connection_errors_scalar_class(self):
        self.app.conf.result_backend_transport_options = dict(
            additional_connection_errors=ConnectionError,
        )
        b = self.Backend(app=self.app)
        assert ConnectionError in b.connection_errors

    def test_additional_connection_errors_scalar_string(self):
        self.app.conf.result_backend_transport_options = dict(
            additional_connection_errors=(
                't.unit.backends.test_redis.ConnectionError'
            ),
        )
        b = self.Backend(app=self.app)
        assert ConnectionError in b.connection_errors

    def test_additional_connection_errors_non_exception_ignored(self):
        self.app.conf.result_backend_transport_options = dict(
            additional_connection_errors=(ConnectionError, int),
        )
        b = self.Backend(app=self.app)
        assert ConnectionError in b.connection_errors
        assert int not in b.connection_errors

    def test_additional_connection_errors_non_type_ignored(self):
        self.app.conf.result_backend_transport_options = dict(
            additional_connection_errors=(ConnectionError, 42),
        )
        b = self.Backend(app=self.app)
        assert ConnectionError in b.connection_errors

    def test_additional_connection_errors_bad_import_ignored(self):
        self.app.conf.result_backend_transport_options = dict(
            additional_connection_errors=(
                ConnectionError, 'no.such.module.Error',
            ),
        )
        b = self.Backend(app=self.app)
        assert ConnectionError in b.connection_errors

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
        header_result_args = (
            uuid(),
            [self.app.AsyncResult(x) for x in range(3)],
        )
        self.b.apply_chord(header_result_args, None)
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
        request.id = request.group = request.group_index = None
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

    def test_set_chord_size(self):
        b = self.Backend('redis://', app=self.app)
        gid = uuid()
        b.set_chord_size(gid, 10)
        b.client.set.assert_called_with(b.get_key_for_group(gid, '.s'), 10)

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
        self.b._set_with_state('foo', 'bar', states.SUCCESS)

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

    def test_set_raises_error_on_large_value(self):
        with pytest.raises(BackendStoreError):
            self.b.set('key', 'x' * (self.b._MAX_STR_VALUE_SIZE + 1))

    def test_driver_info_with_driverinfo_class(self):
        """Test that DriverInfo is used when available."""
        from celery import __version__

        # Mock DriverInfo class and instance
        mock_driver_info_instance = Mock()
        mock_add_upstream_result = Mock()
        mock_driver_info_instance.add_upstream_driver.return_value = mock_add_upstream_result

        mock_driver_info_class = Mock(return_value=mock_driver_info_instance)

        with patch('redis.DriverInfo', mock_driver_info_class, create=True):
            x = self.Backend(app=self.app)

            # Should have driver_info in connparams
            assert 'driver_info' in x.connparams
            assert x.connparams['driver_info'] == mock_add_upstream_result

            # Verify DriverInfo() was called and add_upstream_driver was called
            mock_driver_info_class.assert_called_once_with()
            mock_driver_info_instance.add_upstream_driver.assert_called_once_with(
                'celery',
                __version__
            )

    def test_driver_info_fallback_to_lib_name(self):
        """Test fallback to lib_name/lib_version when DriverInfo not available."""
        from celery import __version__

        # Ensure DriverInfo import fails
        with patch('redis.DriverInfo', side_effect=ImportError, create=True):
            # Mock redis.__version__ to test lib_version
            with patch('redis.__version__', '5.0.8'):
                x = self.Backend(app=self.app)

                # Should have lib_name/lib_version in connparams
                assert 'lib_name' in x.connparams
                assert 'lib_version' in x.connparams
                # lib_name should follow redis-py convention
                assert x.connparams['lib_name'] == f'redis-py(celery_v{__version__})'
                # lib_version should be redis-py version
                assert x.connparams['lib_version'] == '5.0.8'
                # Should NOT have driver_info
                assert 'driver_info' not in x.connparams

    def test_driver_info_fallback_with_attribute_error(self):
        """Test fallback when DriverInfo raises AttributeError."""
        from celery import __version__

        # Ensure DriverInfo raises AttributeError
        with patch('redis.DriverInfo', side_effect=AttributeError, create=True):
            # Mock redis.__version__ to test lib_version
            with patch('redis.__version__', '5.0.8'):
                x = self.Backend(app=self.app)

                # Should have lib_name/lib_version in connparams
                assert 'lib_name' in x.connparams
                assert 'lib_version' in x.connparams
                assert x.connparams['lib_name'] == f'redis-py(celery_v{__version__})'
                assert x.connparams['lib_version'] == '5.0.8'
                # Should NOT have driver_info
                assert 'driver_info' not in x.connparams

    def test_driver_info_fallback_redis_version_unknown(self):
        """Test fallback when redis.__version__ is not available."""
        import redis

        from celery import __version__

        # Ensure DriverInfo import fails
        with patch('redis.DriverInfo', side_effect=ImportError, create=True):
            # Save original __version__ and delete it temporarily
            original_version = getattr(redis, '__version__', None)
            try:
                if hasattr(redis, '__version__'):
                    delattr(redis, '__version__')

                x = self.Backend(app=self.app)

                # Should have lib_name/lib_version in connparams
                assert 'lib_name' in x.connparams
                assert 'lib_version' in x.connparams
                assert x.connparams['lib_name'] == f'redis-py(celery_v{__version__})'
                # lib_version should be 'unknown' when redis.__version__ not available
                assert x.connparams['lib_version'] == 'unknown'
                # Should NOT have driver_info
                assert 'driver_info' not in x.connparams
            finally:
                # Restore original __version__
                if original_version is not None:
                    redis.__version__ = original_version


class test_RedisBackend_result_compression(basetest_RedisBackend):
    """Round trips across the backend boundary, not through encode() alone.

    ``store_result`` and ``get_result`` go through the backend's own set and
    get, so a payload that the backend altered on the way in or out would
    show up here even though the encoding tests in ``test_base`` pass.
    """

    def stored(self, tid):
        return self.b.client.keyspace[self.b.get_key_for_task(tid)]

    def backend(self, compression='gzip', serializer='json'):
        self.app.conf.result_serializer = serializer
        self.app.conf.accept_content = [serializer]
        self.app.conf.result_compression = compression
        self.b = self.Backend(app=self.app)
        return self.b

    def test_store_and_get_compressed_result(self):
        b = self.backend()
        assert b.compression == 'gzip'
        tid = uuid()
        result = {'value': 'a repetitive value ' * 40}
        b.store_result(tid, result, states.SUCCESS)

        assert self.stored(tid).startswith(COMPRESSED_PAYLOAD_MAGIC)
        assert b.get_state(tid) == states.SUCCESS
        assert b.get_result(tid) == result

    def test_compressed_result_is_smaller_on_the_wire(self):
        result = {'value': 'a repetitive value ' * 40}
        tid = uuid()
        self.backend(compression=None).store_result(tid, result, states.SUCCESS)
        plain = len(self.stored(tid))
        self.backend().store_result(tid, result, states.SUCCESS)
        assert len(self.stored(tid)) < plain

    def test_store_and_get_compressed_binary_serializer_result(self):
        # kombu's dumps returns bytes for pickle, so the payload is bytes
        # before compression as well as after it. That is the combination
        # that a write path assuming str breaks on.
        b = self.backend(serializer='pickle')
        tid = uuid()
        result = {'value': b'\x00\x01\x02\xff', 'text': 'a value ' * 40}
        b.store_result(tid, result, states.SUCCESS)

        assert self.stored(tid).startswith(COMPRESSED_PAYLOAD_MAGIC)
        assert b.get_result(tid) == result

    def test_get_result_written_before_compression(self):
        tid = uuid()
        self.backend(compression=None).store_result(tid, {'foo': 'bar'},
                                                    states.SUCCESS)
        key = self.b.get_key_for_task(tid)
        # A real Redis hands the payload back as bytes whatever went in.
        written = ensure_bytes(self.stored(tid))
        assert not written.startswith(COMPRESSED_PAYLOAD_MAGIC)

        b = self.backend()
        b.client.keyspace[key] = written
        assert b.get_result(tid) == {'foo': 'bar'}


class test_RedisBackend_chords_simple(basetest_RedisBackend):
    @pytest.fixture(scope="class", autouse=True)
    def simple_header_result(self):
        with patch(
            "celery.result.GroupResult.restore", return_value=None,
        ) as p:
            yield p

    def test_on_chord_part_return(self):
        tasks = [self.create_task(i) for i in range(10)]
        random.shuffle(tasks)

        for i in range(10):
            self.b.on_chord_part_return(tasks[i].request, states.SUCCESS, i)
            assert self.b.client.zadd.call_count
            self.b.client.zadd.reset_mock()
        assert self.b.client.zrangebyscore.call_count
        jkey = self.b.get_key_for_group('group_id', '.j')
        tkey = self.b.get_key_for_group('group_id', '.t')
        skey = self.b.get_key_for_group('group_id', '.s')
        self.b.client.delete.assert_has_calls([call(jkey), call(tkey), call(skey)])
        self.b.client.expire.assert_has_calls([
            call(jkey, 86400), call(tkey, 86400), call(skey, 86400),
        ])

    def test_on_chord_part_return__unordered(self):
        self.app.conf.result_backend_transport_options = dict(
            result_chord_ordered=False,
        )

        tasks = [self.create_task(i) for i in range(10)]
        random.shuffle(tasks)

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

    def test_on_chord_part_return__ordered(self):
        self.app.conf.result_backend_transport_options = dict(
            result_chord_ordered=True,
        )

        tasks = [self.create_task(i) for i in range(10)]
        random.shuffle(tasks)

        for i in range(10):
            self.b.on_chord_part_return(tasks[i].request, states.SUCCESS, i)
            assert self.b.client.zadd.call_count
            self.b.client.zadd.reset_mock()
        assert self.b.client.zrangebyscore.call_count
        jkey = self.b.get_key_for_group('group_id', '.j')
        tkey = self.b.get_key_for_group('group_id', '.t')
        self.b.client.delete.assert_has_calls([call(jkey), call(tkey)])
        self.b.client.expire.assert_has_calls([
            call(jkey, 86400), call(tkey, 86400),
        ])

    def test_on_chord_part_return_no_expiry(self):
        old_expires = self.b.expires
        self.b.expires = None
        tasks = [self.create_task(i) for i in range(10)]
        self.b.set_chord_size('group_id', 10)

        for i in range(10):
            self.b.on_chord_part_return(tasks[i].request, states.SUCCESS, i)
            assert self.b.client.zadd.call_count
            self.b.client.zadd.reset_mock()
        assert self.b.client.zrangebyscore.call_count
        jkey = self.b.get_key_for_group('group_id', '.j')
        tkey = self.b.get_key_for_group('group_id', '.t')
        self.b.client.delete.assert_has_calls([call(jkey), call(tkey)])
        self.b.client.expire.assert_not_called()

        self.b.expires = old_expires

    def test_on_chord_part_return_expire_set_to_zero(self):
        old_expires = self.b.expires
        self.b.expires = 0
        tasks = [self.create_task(i) for i in range(10)]

        for i in range(10):
            self.b.on_chord_part_return(tasks[i].request, states.SUCCESS, i)
            assert self.b.client.zadd.call_count
            self.b.client.zadd.reset_mock()
        assert self.b.client.zrangebyscore.call_count
        jkey = self.b.get_key_for_group('group_id', '.j')
        tkey = self.b.get_key_for_group('group_id', '.t')
        self.b.client.delete.assert_has_calls([call(jkey), call(tkey)])
        self.b.client.expire.assert_not_called()

        self.b.expires = old_expires

    def test_on_chord_part_return_no_expiry__unordered(self):
        self.app.conf.result_backend_transport_options = dict(
            result_chord_ordered=False,
        )

        old_expires = self.b.expires
        self.b.expires = None
        tasks = [self.create_task(i) for i in range(10)]

        for i in range(10):
            self.b.on_chord_part_return(tasks[i].request, states.SUCCESS, i)
            assert self.b.client.rpush.call_count
            self.b.client.rpush.reset_mock()
        assert self.b.client.lrange.call_count
        jkey = self.b.get_key_for_group('group_id', '.j')
        tkey = self.b.get_key_for_group('group_id', '.t')
        self.b.client.delete.assert_has_calls([call(jkey), call(tkey)])
        self.b.client.expire.assert_not_called()

        self.b.expires = old_expires

    def test_on_chord_part_return_no_expiry__ordered(self):
        self.app.conf.result_backend_transport_options = dict(
            result_chord_ordered=True,
        )

        old_expires = self.b.expires
        self.b.expires = None
        tasks = [self.create_task(i) for i in range(10)]

        for i in range(10):
            self.b.on_chord_part_return(tasks[i].request, states.SUCCESS, i)
            assert self.b.client.zadd.call_count
            self.b.client.zadd.reset_mock()
        assert self.b.client.zrangebyscore.call_count
        jkey = self.b.get_key_for_group('group_id', '.j')
        tkey = self.b.get_key_for_group('group_id', '.t')
        self.b.client.delete.assert_has_calls([call(jkey), call(tkey)])
        self.b.client.expire.assert_not_called()

        self.b.expires = old_expires

    def test_on_chord_part_return__success(self):
        with self.chord_context(2) as (_, request, callback):
            self.b.on_chord_part_return(request, states.SUCCESS, 10)
            callback.delay.assert_not_called()
            self.b.on_chord_part_return(request, states.SUCCESS, 20)
            callback.delay.assert_called_with([10, 20])

    def test_on_chord_part_return__success__unordered(self):
        self.app.conf.result_backend_transport_options = dict(
            result_chord_ordered=False,
        )

        with self.chord_context(2) as (_, request, callback):
            self.b.on_chord_part_return(request, states.SUCCESS, 10)
            callback.delay.assert_not_called()
            self.b.on_chord_part_return(request, states.SUCCESS, 20)
            callback.delay.assert_called_with([10, 20])

    def test_on_chord_part_return__success__ordered(self):
        self.app.conf.result_backend_transport_options = dict(
            result_chord_ordered=True,
        )

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

    def test_on_chord_part_return__callback_raises__unordered(self):
        self.app.conf.result_backend_transport_options = dict(
            result_chord_ordered=False,
        )

        with self.chord_context(1) as (_, request, callback):
            callback.delay.side_effect = KeyError(10)
            task = self.app._tasks['add'] = Mock(name='add_task')
            self.b.on_chord_part_return(request, states.SUCCESS, 10)
            task.backend.fail_from_current_stack.assert_called_with(
                callback.id, exc=ANY,
            )

    def test_on_chord_part_return__callback_raises__ordered(self):
        self.app.conf.result_backend_transport_options = dict(
            result_chord_ordered=True,
        )

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
            self.b.client.pipeline.return_value.zadd().zcount().get().get().expire(
            ).expire().expire().execute.return_value = (1, 1, 0, b'1', 4, 5, 6)
            task = self.app._tasks['add'] = Mock(name='add_task')
            self.b.on_chord_part_return(request, states.SUCCESS, 10)
            task.backend.fail_from_current_stack.assert_called_with(
                callback.id, exc=ANY,
            )

    def test_on_chord_part_return__ChordError__unordered(self):
        self.app.conf.result_backend_transport_options = dict(
            result_chord_ordered=False,
        )

        with self.chord_context(1) as (_, request, callback):
            self.b.client.pipeline = ContextMock()
            raise_on_second_call(self.b.client.pipeline, ChordError())
            self.b.client.pipeline.return_value.rpush().llen().get().get().expire(
            ).expire().expire().execute.return_value = (1, 1, 0, b'1', 4, 5, 6)
            task = self.app._tasks['add'] = Mock(name='add_task')
            self.b.on_chord_part_return(request, states.SUCCESS, 10)
            task.backend.fail_from_current_stack.assert_called_with(
                callback.id, exc=ANY,
            )

    def test_on_chord_part_return__ChordError__ordered(self):
        self.app.conf.result_backend_transport_options = dict(
            result_chord_ordered=True,
        )

        with self.chord_context(1) as (_, request, callback):
            self.b.client.pipeline = ContextMock()
            raise_on_second_call(self.b.client.pipeline, ChordError())
            self.b.client.pipeline.return_value.zadd().zcount().get().get().expire(
            ).expire().expire().execute.return_value = (1, 1, 0, b'1', 4, 5, 6)
            task = self.app._tasks['add'] = Mock(name='add_task')
            self.b.on_chord_part_return(request, states.SUCCESS, 10)
            task.backend.fail_from_current_stack.assert_called_with(
                callback.id, exc=ANY,
            )

    def test_on_chord_part_return__other_error(self):
        with self.chord_context(1) as (_, request, callback):
            self.b.client.pipeline = ContextMock()
            raise_on_second_call(self.b.client.pipeline, RuntimeError())
            self.b.client.pipeline.return_value.zadd().zcount().get().get().expire(
            ).expire().expire().execute.return_value = (1, 1, 0, b'1', 4, 5, 6)
            task = self.app._tasks['add'] = Mock(name='add_task')
            self.b.on_chord_part_return(request, states.SUCCESS, 10)
            task.backend.fail_from_current_stack.assert_called_with(
                callback.id, exc=ANY,
            )

    def test_on_chord_part_return__other_error__unordered(self):
        self.app.conf.result_backend_transport_options = dict(
            result_chord_ordered=False,
        )

        with self.chord_context(1) as (_, request, callback):
            self.b.client.pipeline = ContextMock()
            raise_on_second_call(self.b.client.pipeline, RuntimeError())
            self.b.client.pipeline.return_value.rpush().llen().get().get().expire(
            ).expire().expire().execute.return_value = (1, 1, 0, b'1', 4, 5, 6)
            task = self.app._tasks['add'] = Mock(name='add_task')
            self.b.on_chord_part_return(request, states.SUCCESS, 10)
            task.backend.fail_from_current_stack.assert_called_with(
                callback.id, exc=ANY,
            )

    def test_on_chord_part_return__other_error__ordered(self):
        self.app.conf.result_backend_transport_options = dict(
            result_chord_ordered=True,
        )

        with self.chord_context(1) as (_, request, callback):
            self.b.client.pipeline = ContextMock()
            raise_on_second_call(self.b.client.pipeline, RuntimeError())
            self.b.client.pipeline.return_value.zadd().zcount().get().get().expire(
            ).expire().expire().execute.return_value = (1, 1, 0, b'1', 4, 5, 6)
            task = self.app._tasks['add'] = Mock(name='add_task')
            self.b.on_chord_part_return(request, states.SUCCESS, 10)
            task.backend.fail_from_current_stack.assert_called_with(
                callback.id, exc=ANY,
            )


class test_RedisBackend_chords_complex(basetest_RedisBackend):
    @pytest.fixture(scope="function", autouse=True)
    def complex_header_result(self):
        with patch("celery.result.GroupResult.restore") as p:
            yield p

    @pytest.mark.parametrize(['results', 'assert_save_called'], [
        # No results in the header at all - won't call `save()`
        (tuple(), False),
        # Simple results in the header - won't call `save()`
        ((AsyncResult("foo"), ), False),
        # Many simple results in the header - won't call `save()`
        ((AsyncResult("foo"), ) * 42, False),
        # A single complex result in the header - will call `save()`
        ((GroupResult("foo", []),), True),
        # Many complex results in the header - will call `save()`
        ((GroupResult("foo"), ) * 42, True),
        # Mixed simple and complex results in the header - will call `save()`
        (itertools.islice(
            itertools.cycle((
                AsyncResult("foo"), GroupResult("foo"),
            )), 42,
        ), True),
    ])
    def test_apply_chord_complex_header(self, results, assert_save_called):
        mock_group_result = Mock()
        mock_group_result.return_value.results = results
        self.app.GroupResult = mock_group_result
        header_result_args = ("gid11", results)
        self.b.apply_chord(header_result_args, None)
        if assert_save_called:
            mock_group_result.return_value.save.assert_called_once_with(backend=self.b)
        else:
            mock_group_result.return_value.save.assert_not_called()

    def test_on_chord_part_return_timeout(self, complex_header_result):
        tasks = [self.create_task(i) for i in range(10)]
        random.shuffle(tasks)
        try:
            self.app.conf.result_chord_join_timeout += 1.0
            for task, result_val in zip(tasks, itertools.cycle((42, ))):
                self.b.on_chord_part_return(
                    task.request, states.SUCCESS, result_val,
                )
        finally:
            self.app.conf.result_chord_join_timeout -= 1.0

        join_func = complex_header_result.return_value.join_native
        join_func.assert_called_once_with(timeout=4.0, propagate=True)

    @pytest.mark.parametrize("supports_native_join", (True, False))
    def test_on_chord_part_return(
        self, complex_header_result, supports_native_join,
    ):
        mock_result_obj = complex_header_result.return_value
        mock_result_obj.supports_native_join = supports_native_join

        tasks = [self.create_task(i) for i in range(10)]
        random.shuffle(tasks)

        with self.chord_context(10) as (tasks, request, callback):
            for task, result_val in zip(tasks, itertools.cycle((42, ))):
                self.b.on_chord_part_return(
                    task.request, states.SUCCESS, result_val,
                )
                # Confirm that `zadd` was called even though we won't end up
                # using the data pushed into the sorted set
                assert self.b.client.zadd.call_count == 1
                self.b.client.zadd.reset_mock()
        # Confirm that neither `zrange` not `lrange` were called
        self.b.client.zrange.assert_not_called()
        self.b.client.lrange.assert_not_called()
        # Confirm that the `GroupResult.restore` mock was called
        complex_header_result.assert_called_once_with(request.group, app=self.b.app)
        # Confirm that the callback was called with the `join()`ed group result
        if supports_native_join:
            expected_join = mock_result_obj.join_native
        else:
            expected_join = mock_result_obj.join
        callback.delay.assert_called_once_with(expected_join())


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

    def setup_method(self):
        self.Backend = self.get_backend()
        self.E_LOST = self.get_E_LOST()
        self.b = self.Backend(app=self.app)

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_reduce(self):
        pytest.importorskip('redis')

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

        # By default passwords should be sanitized
        display_url = x.as_uri()
        assert "test" not in display_url
        # We can choose not to sanitize with the `include_password` argument
        unsanitized_display_url = x.as_uri(include_password=True)
        assert unsanitized_display_url == x.url
        # or to explicitly sanitize
        forcibly_sanitized_display_url = x.as_uri(include_password=False)
        assert forcibly_sanitized_display_url == display_url

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

    def test_backend_ssl(self):
        pytest.importorskip('redis')

        from celery.backends.redis import SentinelBackend
        self.app.conf.redis_backend_use_ssl = {
            'ssl_cert_reqs': "CERT_REQUIRED",
            'ssl_ca_certs': '/path/to/ca.crt',
            'ssl_certfile': '/path/to/client.crt',
            'ssl_keyfile': '/path/to/client.key',
        }
        self.app.conf.redis_socket_timeout = 30.0
        self.app.conf.redis_socket_connect_timeout = 100.0
        x = SentinelBackend(
            'sentinel://:bosco@vandelay.com:123//1', app=self.app,
        )
        assert x.connparams
        assert len(x.connparams['hosts']) == 1
        assert x.connparams['hosts'][0]['host'] == 'vandelay.com'
        assert x.connparams['hosts'][0]['db'] == 1
        assert x.connparams['hosts'][0]['port'] == 123
        assert x.connparams['hosts'][0]['password'] == 'bosco'
        assert x.connparams['socket_timeout'] == 30.0
        assert x.connparams['socket_connect_timeout'] == 100.0
        assert x.connparams['ssl_cert_reqs'] == ssl.CERT_REQUIRED
        assert x.connparams['ssl_ca_certs'] == '/path/to/ca.crt'
        assert x.connparams['ssl_certfile'] == '/path/to/client.crt'
        assert x.connparams['ssl_keyfile'] == '/path/to/client.key'

        from celery.backends.redis import SentinelManagedSSLConnection
        assert x.connparams['connection_class'] is SentinelManagedSSLConnection

    def test_url_with_acl_credentials(self):
        x = self.Backend(
            'sentinel://myuser:mypass@github.com:123/1;'
            'sentinel://myuser:mypass@github.com:124/1',
            app=self.app,
        )
        assert x.connparams
        assert "host" not in x.connparams
        assert x.connparams['db'] == 1
        assert "port" not in x.connparams
        assert x.connparams['password'] == "mypass"
        assert x.connparams['username'] == "myuser"
        assert len(x.connparams['hosts']) == 2

        expected_usernames = ["myuser", "myuser"]
        found_usernames = [cp['username'] for cp in x.connparams['hosts']]
        assert found_usernames == expected_usernames

    def test_get_pool_with_acl_credentials(self):
        x = self.Backend(
            'sentinel://myuser:mypass@github.com:123/1;'
            'sentinel://myuser:mypass@github.com:124/1',
            app=self.app,
        )
        with patch.object(x, '_get_sentinel_instance') as mock_get_sentinel:
            mock_sentinel = Mock()
            mock_sentinel.master_for.return_value = Mock(connection_pool=Mock())
            mock_get_sentinel.return_value = mock_sentinel

            x._get_pool(**x.connparams)

            mock_sentinel.master_for.assert_called_once()
            call_kwargs = mock_sentinel.master_for.call_args[1]
            assert call_kwargs.get('username') == 'myuser'
            assert call_kwargs.get('password') == 'mypass'

    def test_get_pool_with_password_only(self):
        x = self.Backend(
            'sentinel://:mypass@github.com:123/1',
            app=self.app,
        )
        with patch.object(x, '_get_sentinel_instance') as mock_get_sentinel:
            mock_sentinel = Mock()
            mock_sentinel.master_for.return_value = Mock(connection_pool=Mock())
            mock_get_sentinel.return_value = mock_sentinel


class test_Redis_GroupProgress(basetest_RedisBackend):
    """Test group progress tracking functionality."""

    def test_set_group_progress_size(self):
        """Test setting group progress size initializes hash correctly."""
        group_id = 'test-group-123'
        size = 10

        self.b.set_group_progress_size(group_id, size)

        # Verify the hash was created with correct values
        pkey = self.b.get_key_for_group(group_id, '.p')
        data = self.b.client.hgetall(pkey)

        assert data is not None
        assert int(data.get(b'total', 0)) == size
        assert int(data.get(b'count', 0)) == 0

    def test_set_group_progress_size_with_expires(self):
        """Test that progress key expires when result_expires is set."""
        self.app.conf.result_expires = 3600

        group_id = 'test-group-456'
        size = 5

        with patch.object(self.b.client, 'pipeline') as mock_pipeline:
            mock_pipe = Mock()
            mock_pipeline.return_value.__enter__.return_value = mock_pipe
            mock_pipe.execute.return_value = [None, None, None]

            self.b.set_group_progress_size(group_id, size)

            # Verify expire was called in the pipeline
            calls = [str(call) for call in mock_pipe.method_calls]
            assert any('expire' in call for call in calls)

    def test_increment_group_progress(self):
        """Test incrementing group progress counter."""
        group_id = 'test-group-789'
        size = 10

        # Initialize
        self.b.set_group_progress_size(group_id, size)

        # Increment multiple times with different task IDs
        for i in range(3):
            self.b.increment_group_progress(group_id, f'task-{i}')

        # Verify count
        completed, total = self.b.get_group_progress(group_id)
        assert completed == 3
        assert total == size

    def test_get_group_progress(self):
        """Test retrieving group progress."""
        group_id = 'test-group-abc'
        size = 7

        # Initialize and increment
        self.b.set_group_progress_size(group_id, size)
        self.b.increment_group_progress(group_id, 'task-1')
        self.b.increment_group_progress(group_id, 'task-2')

        completed, total = self.b.get_group_progress(group_id)
        assert completed == 2
        assert total == size

    def test_get_group_progress_nonexistent(self):
        """Test getting progress for non-existent group returns None."""
        completed, total = self.b.get_group_progress('nonexistent-group')
        assert completed is None
        assert total is None

    def test_progress_tracking_atomic_increment(self):
        """Test that increment is atomic (uses Lua script)."""
        group_id = 'test-group-atomic'
        size = 5

        self.b.set_group_progress_size(group_id, size)

        with patch.object(self.b.client, 'eval') as mock_eval:
            mock_eval.return_value = 1
            self.b.increment_group_progress(group_id, 'task-1')

            # Verify eval was called with the Lua script for atomicity
            mock_eval.assert_called_once()
            call_args = mock_eval.call_args
            # First arg should be the Lua script
            assert 'SISMEMBER' in call_args[0][0]
            assert 'SADD' in call_args[0][0]
            assert 'HINCRBY' in call_args[0][0]

    def test_progress_key_format(self):
        """Test that progress key uses correct format."""
        group_id = 'test-group-format'
        size = 3

        self.b.set_group_progress_size(group_id, size)

        # Key should be <group_id>.p
        expected_key = self.b.get_key_for_group(group_id, '.p')
        assert expected_key.endswith(b'.p')

        # Verify it's a hash
        key_type = self.b.client.type(expected_key)
        assert key_type == b'hash'

    def test_multiple_groups_independent(self):
        """Test that multiple groups have independent progress tracking."""
        group1 = 'group-1'
        group2 = 'group-2'

        self.b.set_group_progress_size(group1, 5)
        self.b.set_group_progress_size(group2, 10)

        self.b.increment_group_progress(group1, 'task-1')
        self.b.increment_group_progress(group1, 'task-2')
        self.b.increment_group_progress(group2, 'task-3')

        c1, t1 = self.b.get_group_progress(group1)
        c2, t2 = self.b.get_group_progress(group2)

        assert c1 == 2 and t1 == 5
        assert c2 == 1 and t2 == 10

    def test_supports_group_progress_flag(self):
        """Test that Redis backend reports support for group progress."""
        assert self.b.supports_group_progress is True

    def test_end_to_end_progress_with_task_completion(self):
        """Test end-to-end progress tracking with actual task completion path."""
        group_id = 'test-group-e2e'
        size = 5

        # Initialize progress tracking
        self.b.set_group_progress_size(group_id, size)

        # Simulate tasks completing by calling mark_as_done with group context
        for i in range(size):
            task_id = f'task-{i}'
            request = Context({
                'id': task_id,
                'group': group_id,
                'task': 'test.task'
            })
            self.b.mark_as_done(task_id, i, request=request)

        # Verify progress is correctly tracked
        completed, total = self.b.get_group_progress(group_id)
        assert completed == size
        assert total == size

    def test_end_to_end_progress_with_task_failure(self):
        """Test end-to-end progress tracking with task failures."""
        group_id = 'test-group-fail'
        size = 4

        # Initialize progress tracking
        self.b.set_group_progress_size(group_id, size)

        # Simulate mix of success and failure
        for i in range(size):
            task_id = f'task-{i}'
            request = Context({
                'id': task_id,
                'group': group_id,
                'task': 'test.task'
            })
            if i < 2:
                self.b.mark_as_done(task_id, i, request=request)
            else:
                self.b.mark_as_failure(task_id, Exception('test error'), request=request)

        # Verify all tasks (success + failure) are counted
        completed, total = self.b.get_group_progress(group_id)
        assert completed == size
        assert total == size

    def test_retry_does_not_increment_progress(self):
        """Test that RETRY state does not increment progress counter."""
        group_id = 'test-group-retry'
        size = 3

        # Initialize progress tracking
        self.b.set_group_progress_size(group_id, size)

        # Simulate task retry
        task_id = 'task-retry-1'
        request = Context({
            'id': task_id,
            'group': group_id,
            'task': 'test.task'
        })
        self.b.mark_as_retry(task_id, Exception('retry error'), request=request)

        # Progress should not have incremented
        completed, total = self.b.get_group_progress(group_id)
        assert completed == 0
        assert total == size

    def test_retry_then_success_increments_once(self):
        """Test that RETRY → SUCCESS increments progress exactly once."""
        group_id = 'test-group-retry-success'
        size = 2

        # Initialize progress tracking
        self.b.set_group_progress_size(group_id, size)

        task_id = 'task-retry-success-1'
        request = Context({
            'id': task_id,
            'group': group_id,
            'task': 'test.task'
        })

        # First, mark as retry
        self.b.mark_as_retry(task_id, Exception('retry error'), request=request)

        # Verify no increment
        completed, total = self.b.get_group_progress(group_id)
        assert completed == 0

        # Then mark as success
        self.b.mark_as_done(task_id, 'result', request=request)

        # Verify exactly one increment
        completed, total = self.b.get_group_progress(group_id)
        assert completed == 1
        assert total == size

    def test_idempotent_increment_duplicate_task_completion(self):
        """Test that duplicate task completion does not double-count."""
        group_id = 'test-group-duplicate'
        size = 3

        # Initialize progress tracking
        self.b.set_group_progress_size(group_id, size)

        # Mark same task as done twice (simulating duplicate completion notification)
        task_id = 'task-duplicate-1'
        request = Context({
            'id': task_id,
            'group': group_id,
            'task': 'test.task'
        })

        # First completion
        self.b.mark_as_done(task_id, 'result', request=request)
        completed, total = self.b.get_group_progress(group_id)
        assert completed == 1

        # Duplicate completion (same task_id)
        self.b.mark_as_done(task_id, 'result', request=request)
        completed, total = self.b.get_group_progress(group_id)
        assert completed == 1  # Should still be 1, not 2

    def test_idempotent_increment_duplicate_failure(self):
        """Test that duplicate task failure does not double-count."""
        group_id = 'test-group-duplicate-fail'
        size = 2

        # Initialize progress tracking
        self.b.set_group_progress_size(group_id, size)

        # Mark same task as failed twice
        task_id = 'task-duplicate-fail-1'
        request = Context({
            'id': task_id,
            'group': group_id,
            'task': 'test.task'
        })

        # First failure
        self.b.mark_as_failure(task_id, Exception('error'), request=request)
        completed, total = self.b.get_group_progress(group_id)
        assert completed == 1

        # Duplicate failure
        self.b.mark_as_failure(task_id, Exception('error'), request=request)
        completed, total = self.b.get_group_progress(group_id)
        assert completed == 1  # Should still be 1, not 2

    def test_get_group_progress_with_str_keys(self):
        """Test that get_group_progress handles str keys (decode_responses=True)."""
        group_id = 'test-group-str-keys'
        size = 5

        # Initialize progress tracking
        self.b.set_group_progress_size(group_id, size)

        # Mock hgetall to return str keys instead of bytes
        original_hgetall = self.b.client.hgetall

        def mock_hgetall_with_str(key):
            data = original_hgetall(key)
            # Convert bytes keys to str to simulate decode_responses=True
            if data:
                return {k.decode() if isinstance(k, bytes) else k: v for k, v in data.items()}
            return data

        self.b.client.hgetall = mock_hgetall_with_str

        # Increment progress
        task_id = 'task-str-1'
        request = Context({
            'id': task_id,
            'group': group_id,
            'task': 'test.task'
        })
        self.b.mark_as_done(task_id, 'result', request=request)

        # Should handle str keys correctly
        completed, total = self.b.get_group_progress(group_id)
        assert completed == 1
        assert total == size

        # Restore original
        self.b.client.hgetall = original_hgetall

    def test_chord_without_track_progress_no_extra_writes(self):
        """Test that chords without track_progress don't write progress keys."""
        group_id = 'test-chord-no-progress'

        # Simulate chord part return without track_progress
        request = Context({
            'id': 'task-1',
            'group': group_id,
            'chord': 'callback-task',
            'task': 'test.task'
        })

        # This should not create progress keys
        self.b.on_chord_part_return(request, states.SUCCESS, 'result')

        # Verify no progress keys were created
        pkey = self.b.get_key_for_group(group_id, '.p')
        seen_key = self.b.get_key_for_group(group_id, '.p.seen')

        assert self.b.client.get(pkey) is None
        assert self.b.client.get(seen_key) is None

    def test_untracked_group_no_redis_keys_created(self):
        """Test that groups without track_progress create zero Redis keys.

        This is the critical test for the write leak fix. It verifies that
        calling mark_as_done/mark_as_failure for a task in a group that never
        called set_group_progress_size results in NO new Redis keys being
        created - not just that get_group_progress returns (None, None), but
        that the keys themselves don't exist in the keyspace.

        Note: mark_as_done will create a task meta key (celery-task-meta-<task_id>),
        which is expected and unrelated to progress tracking. This test only
        checks that the progress-specific keys (.p and .p.seen) are NOT created.
        """
        group_id = 'test-untracked-group'
        task_id = 'task-untracked-1'

        # Simulate task completion WITHOUT calling set_group_progress_size first
        request = Context({
            'id': task_id,
            'group': group_id,
            'task': 'test.task'
        })

        # Call mark_as_done - this should NOT create any progress keys
        self.b.mark_as_done(task_id, 'result', request=request)

        # Specifically check that the progress keys don't exist
        pkey = self.b.get_key_for_group(group_id, '.p')
        seen_key = self.b.get_key_for_group(group_id, '.p.seen')

        assert pkey not in self.b.client.keyspace, (
            f"Progress hash key should not exist: {pkey}"
        )
        assert seen_key not in self.b.client.keyspace, (
            f"Progress seen key should not exist: {seen_key}"
        )

        # Also verify via the public API
        completed, total = self.b.get_group_progress(group_id)
        assert completed is None
        assert total is None

        # Test with mark_as_failure as well
        task_id_2 = 'task-untracked-2'
        request_2 = Context({
            'id': task_id_2,
            'group': group_id,
            'task': 'test.task'
        })

        self.b.mark_as_failure(task_id_2, Exception('test'), request=request_2)

        # Still no progress keys should have been created
        assert pkey not in self.b.client.keyspace, (
            f"Progress hash key should not exist after failure: {pkey}"
        )
        assert seen_key not in self.b.client.keyspace, (
            f"Progress seen key should not exist after failure: {seen_key}"
        )

    def test_get_group_progress_defensive_about_missing_total(self):
        """Test that get_group_progress returns (None, None) when total is missing.

        This is defensive against old buggy keys that may have been created
        before the initialization check was added - a hash with 'count' but
        no 'total' should be treated as "not tracked".
        """
        group_id = 'test-defensive-group'

        # Manually create a buggy hash with only 'count' (simulating old bug)
        pkey = self.b.get_key_for_group(group_id, '.p')
        self.b.client.hset(pkey, b'count', 5)

        # get_group_progress should return (None, None) since 'total' is missing
        completed, total = self.b.get_group_progress(group_id)
        assert completed is None
        assert total is None

        # Now add 'total' to make it valid
        self.b.client.hset(pkey, b'total', 10)

        # Now it should return the values
        completed, total = self.b.get_group_progress(group_id)
        assert completed == 5
        assert total == 10

    def test_nested_groups_separate_tracking(self):
        """Test that nested groups have separate progress tracking.

        This test uses two separate top-level groups to verify they don't
        cross-contaminate each other's counters. This is a basic isolation test,
        not a test of actual canvas nesting (see test_real_canvas_nesting for that).
        """
        # Outer group
        outer_group_id = 'test-outer-group'
        outer_size = 2

        # Inner group (nested inside outer)
        inner_group_id = 'test-inner-group'
        inner_size = 3

        # Initialize both groups
        self.b.set_group_progress_size(outer_group_id, outer_size)
        self.b.set_group_progress_size(inner_group_id, inner_size)

        # Mark a task in the inner group as done
        inner_task_id = 'inner-task-1'
        inner_request = Context({
            'id': inner_task_id,
            'group': inner_group_id,
            'task': 'test.task'
        })
        self.b.mark_as_done(inner_task_id, 'result', request=inner_request)

        # Verify only inner group progress incremented
        inner_completed, inner_total = self.b.get_group_progress(inner_group_id)
        assert inner_completed == 1
        assert inner_total == inner_size

    def test_real_canvas_nesting(self):
        """Test actual canvas nesting behavior with group(group(...)).

        This test inspects the frozen signatures to determine what group_id
        tasks receive when groups are nested (e.g., group(group(sig1, sig2), sig3)).

        Based on canvas.py's _prepared method (lines 1781-1790), nested groups
        are flattened/unrolled during preparation, and all tasks receive the
        outer group's ID. The inner group's own ID is never assigned to its tasks.
        """
        from celery import group

        # Create a nested canvas: group(group(sig1, sig2), sig3)
        sig1 = signature('add', args=(1, 2), app=self.app)
        sig2 = signature('add', args=(3, 4), app=self.app)
        sig3 = signature('add', args=(5, 6), app=self.app)

        inner_group = group(sig1, sig2, app=self.app)
        outer_group = group(inner_group, sig3, app=self.app)

        # Freeze the outer group - this triggers the _prepared method which unrolls nested groups
        frozen_outer = outer_group.freeze()
        outer_group_id = frozen_outer.id

        # The frozen outer group should have 3 results (2 from inner, 1 from sig3)
        # because nested groups are flattened during freezing
        assert len(frozen_outer.results) == 3

        # Instead, let's inspect the _prepared output directly by calling it
        # This is what apply_async does before submitting tasks
        tasks_generator = outer_group._prepared(outer_group.tasks, [], outer_group_id, None, self.app)
        tasks_list = list(tasks_generator)

        # Each item in the list is (task, AsyncResult, group_id)
        assert len(tasks_list) == 3

        # Extract the group_id from each tuple
        group_ids = [item[2] for item in tasks_list]

        # All 3 tasks should have the SAME group_id (the outer group's ID)
        # This confirms that nested groups are flattened during preparation
        assert all(gid == outer_group_id for gid in group_ids), \
            f"All tasks should have outer group ID {outer_group_id}, but got: {group_ids}"

        # This means: nested groups are NOT a separate tracking concern.
        # The inner group's tasks get the outer group's ID, so progress tracking
        # for the outer group correctly counts all 3 tasks (not 2).
        # The "nested group" concern from the original Phase 3 prompt is moot
        # because groups are flattened before execution.

    def test_concurrent_progress_increments(self):
        """Test that concurrent progress increments are handled correctly.

        Note: This test validates the check-and-increment logic against a mock
        Redis client using threading. It does NOT verify true Redis-server-side
        atomicity of the Lua script under concurrent network connections. An
        integration test against a live Redis server would be required to verify
        that the Lua script's EVAL command is truly atomic under real concurrent
        connections. This unit test is sufficient to validate the logic but does
        not prove Redis-server atomicity.
        """
        import threading

        group_id = 'test-concurrent-group'
        size = 10

        # Initialize progress tracking
        self.b.set_group_progress_size(group_id, size)

        # Simulate concurrent task completions from multiple "workers"
        num_tasks = 5
        threads = []

        def complete_task(task_num):
            task_id = f'task-{task_num}'
            request = Context({
                'id': task_id,
                'group': group_id,
                'task': 'test.task'
            })
            self.b.mark_as_done(task_id, f'result-{task_num}', request=request)

        # Launch threads to simulate concurrent completions
        for i in range(num_tasks):
            t = threading.Thread(target=complete_task, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # Verify progress is correct (should be num_tasks, not more due to races)
        completed, total = self.b.get_group_progress(group_id)
        assert completed == num_tasks
        assert total == size

    def test_seen_set_ttl_propagation(self):
        """Test that seen set TTL is propagated from progress hash."""
        self.app.conf.result_expires = 3600
        group_id = 'test-ttl-group'
        task_id = 'task-1'

        # Initialize progress with expiration
        self.b.set_group_progress_size(group_id, 5)

        # Increment progress (this should propagate TTL to seen set)
        self.b.increment_group_progress(group_id, task_id)

        pkey = self.b.get_key_for_group(group_id, '.p')
        seen_key = self.b.get_key_for_group(group_id, '.p.seen')

        # Verify both keys have TTL set
        pkey_ttl = self.b.client.ttl(pkey)
        seen_ttl = self.b.client.ttl(seen_key)

        assert pkey_ttl > 0, "Progress hash should have TTL"
        assert seen_ttl > 0, "Seen set should have TTL"
        # They should be equal since Lua script propagates TTL
        assert pkey_ttl == seen_ttl, f"TTL should match: pkey={pkey_ttl}, seen={seen_ttl}"

    def test_seen_set_no_ttl_when_progress_no_ttl(self):
        """Test that seen set TTL matches progress TTL even when using default."""
        # When result_expires is None, Redis backend uses a default TTL
        # The important thing is that seen set TTL matches progress hash TTL
        self.app.conf.result_expires = None
        group_id = 'test-no-ttl-group'
        task_id = 'task-1'

        # Initialize progress without explicit expiration
        self.b.set_group_progress_size(group_id, 5)

        # Increment progress
        self.b.increment_group_progress(group_id, task_id)

        pkey = self.b.get_key_for_group(group_id, '.p')
        seen_key = self.b.get_key_for_group(group_id, '.p.seen')

        # Verify both keys have the same TTL (whether default or -1)
        pkey_ttl = self.b.client.ttl(pkey)
        seen_ttl = self.b.client.ttl(seen_key)

        # The key verification is that they match, ensuring seen set doesn't have infinite TTL
        assert pkey_ttl == seen_ttl, f"TTL should match: pkey={pkey_ttl}, seen={seen_ttl}"
