import socket
import uuid
from unittest.mock import Mock, patch

import pytest

from celery import chord, group
from celery._state import _task_stack
from celery.backends.rpc import RPCBackend


class test_RPCResultConsumer:
    def get_backend(self):
        return RPCBackend(app=self.app)

    def get_consumer(self):
        return self.get_backend().result_consumer

    def test_drain_events_before_start(self):
        consumer = self.get_consumer()
        # drain_events shouldn't crash when called before start
        consumer.drain_events(0.001)

    def test_drain_events_reconnects_on_connection_error(self):
        consumer = self.get_consumer()
        # Simulate a started consumer with a live connection.
        mock_conn = Mock(name='connection')
        mock_conn.connection_errors = (OSError,)
        mock_conn.channel_errors = ()
        mock_conn.drain_events.side_effect = OSError(
            'Server unexpectedly closed connection'
        )
        consumer._connection = mock_conn
        consumer._connection_errors = mock_conn.connection_errors + mock_conn.channel_errors

        mock_consumer = Mock(name='consumer')
        mock_consumer.queues = [Mock(name='queue1')]
        consumer._consumer = mock_consumer

        # Patch app.connection() to return a fresh mock connection
        # and Consumer to return a mock consumer.
        new_conn = Mock(name='new_connection')
        new_conn.connection_errors = (OSError,)
        new_conn.channel_errors = ()
        new_kombu_consumer = Mock(name='new_kombu_consumer')
        consumer.app = Mock()
        consumer.app.connection.return_value = new_conn
        consumer.Consumer = Mock(return_value=new_kombu_consumer)

        # drain_events should NOT raise; it should reconnect instead.
        consumer.drain_events(timeout=1)

        # Old connection should be closed.
        mock_conn.close.assert_called_once()
        # New connection should be established.
        consumer.app.connection.assert_called_once()
        assert consumer._connection is new_conn
        # New consumer should be consuming.
        assert consumer._consumer is new_kombu_consumer
        new_kombu_consumer.consume.assert_called_once()

    def test_drain_events_reconnect_preserves_queues(self):
        consumer = self.get_consumer()
        mock_conn = Mock(name='connection')
        mock_conn.connection_errors = (ConnectionError,)
        mock_conn.channel_errors = ()
        mock_conn.drain_events.side_effect = ConnectionError('reset')
        consumer._connection = mock_conn
        consumer._connection_errors = mock_conn.connection_errors + mock_conn.channel_errors

        queue1, queue2 = Mock(name='q1'), Mock(name='q2')
        mock_consumer = Mock(name='consumer')
        mock_consumer.queues = [queue1, queue2]
        consumer._consumer = mock_consumer

        new_conn = Mock(name='new_connection')
        new_conn.connection_errors = (ConnectionError,)
        new_conn.channel_errors = ()
        consumer.app = Mock()
        consumer.app.connection.return_value = new_conn
        consumer.Consumer = Mock(return_value=Mock(name='new_kombu_consumer'))

        consumer.drain_events(timeout=1)

        # The new Consumer should have been created with both old queues.
        new_consumer_call = consumer.Consumer.call_args
        assert list(new_consumer_call[0][1]) == [queue1, queue2]

    def test_drain_events_timeout_swallowed(self):
        """socket.timeout from drain_events should NOT trigger reconnection."""
        consumer = self.get_consumer()
        mock_conn = Mock(name='connection')
        mock_conn.connection_errors = (OSError,)
        mock_conn.channel_errors = ()
        # Simulate a normal polling timeout (no messages within timeout period)
        mock_conn.drain_events.side_effect = socket.timeout()
        consumer._connection = mock_conn
        consumer._connection_errors = mock_conn.connection_errors + mock_conn.channel_errors

        mock_consumer = Mock(name='consumer')
        mock_consumer.queues = [Mock(name='queue1')]
        consumer._consumer = mock_consumer

        # drain_events should raise socket.timeout, NOT trigger reconnection.
        # socket.timeout should bubble up so the Drainer can catch it.
        with pytest.raises(socket.timeout):
            consumer.drain_events(timeout=1)

        # Verify that NO reconnection occurred:
        # - Connection should NOT be closed
        mock_conn.close.assert_not_called()

    def test_drain_events_no_reconnect_on_other_errors(self):
        consumer = self.get_consumer()
        mock_conn = Mock(name='connection')
        mock_conn.connection_errors = (OSError,)
        mock_conn.channel_errors = ()
        mock_conn.drain_events.side_effect = RuntimeError('unexpected')
        consumer._connection = mock_conn
        consumer._connection_errors = mock_conn.connection_errors + mock_conn.channel_errors

        with pytest.raises(RuntimeError, match='unexpected'):
            consumer.drain_events(timeout=1)

    def test_reconnect_handles_close_failures_gracefully(self):
        consumer = self.get_consumer()
        mock_conn = Mock(name='connection')
        mock_conn.close.side_effect = OSError('already closed')
        consumer._connection = mock_conn

        mock_consumer = Mock(name='consumer')
        mock_consumer.cancel.side_effect = OSError('channel gone')
        mock_consumer.queues = [Mock(name='queue1')]
        consumer._consumer = mock_consumer

        new_conn = Mock(name='new_connection')
        new_conn.connection_errors = (OSError,)
        new_conn.channel_errors = ()
        new_kombu_consumer = Mock(name='new_kombu_consumer')
        consumer.app = Mock()
        consumer.app.connection.return_value = new_conn
        consumer.Consumer = Mock(return_value=new_kombu_consumer)

        # _reconnect should NOT raise even if cancel/close fail
        consumer._reconnect()

        assert consumer._connection is new_conn
        new_kombu_consumer.consume.assert_called_once()

    def test_drain_events_channel_error_triggers_reconnect(self):
        consumer = self.get_consumer()
        mock_conn = Mock(name='connection')
        mock_conn.connection_errors = ()
        mock_conn.channel_errors = (KeyError,)
        mock_conn.drain_events.side_effect = KeyError('channel closed')
        consumer._connection = mock_conn
        consumer._connection_errors = mock_conn.connection_errors + mock_conn.channel_errors

        mock_consumer = Mock(name='consumer')
        mock_consumer.queues = []
        consumer._consumer = mock_consumer

        new_conn = Mock(name='new_connection')
        new_conn.connection_errors = ()
        new_conn.channel_errors = (KeyError,)
        consumer.app = Mock()
        consumer.app.connection.return_value = new_conn
        consumer.Consumer = Mock(return_value=Mock(name='new_kombu_consumer'))

        consumer.drain_events(timeout=1)

        assert consumer._connection is new_conn

    def test_drain_events_raises_runtime_when_reconnect_also_fails(self):
        consumer = self.get_consumer()

        class FakeConnError(Exception):
            pass

        mock_conn = Mock(name='connection')
        mock_conn.connection_errors = (FakeConnError,)
        mock_conn.channel_errors = ()
        mock_conn.drain_events.side_effect = FakeConnError('dropped')
        consumer._connection = mock_conn
        consumer._connection_errors = mock_conn.connection_errors + mock_conn.channel_errors

        mock_consumer = Mock(name='consumer')
        mock_consumer.queues = []
        consumer._consumer = mock_consumer

        consumer.app = Mock()
        consumer.app.connection.side_effect = FakeConnError('still down')

        with pytest.raises(RuntimeError, match='Retry limit exceeded'):
            consumer.drain_events(timeout=1)


class test_RPCBackend:

    def setup_method(self):
        self.b = RPCBackend(app=self.app)

    def test_oid(self):
        oid = self.b.oid
        oid2 = self.b.oid
        assert uuid.UUID(oid)
        assert oid == oid2
        assert oid == self.app.thread_oid

    def test_oid_threads(self):
        # Verify that two RPC backends executed in different threads
        # has different oid.
        oid = self.b.oid
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(lambda: RPCBackend(app=self.app).oid)
        thread_oid = future.result()
        assert uuid.UUID(oid)
        assert uuid.UUID(thread_oid)
        assert oid == self.app.thread_oid
        assert thread_oid != oid

    def test_interface(self):
        self.b.on_reply_declare('task_id')

    def test_ensure_chords_allowed(self):
        with pytest.raises(NotImplementedError):
            self.b.ensure_chords_allowed()

    def test_apply_chord(self):
        with pytest.raises(NotImplementedError):
            self.b.apply_chord(self.app.GroupResult(), None)

    @pytest.mark.celery(result_backend='rpc')
    def test_chord_raises_error(self):
        with pytest.raises(NotImplementedError):
            chord(self.add.s(i, i) for i in range(10))(self.add.s([2]))

    @pytest.mark.celery(result_backend='rpc')
    def test_chain_with_chord_raises_error(self):
        with pytest.raises(NotImplementedError):
            (self.add.s(2, 2) |
             group(self.add.s(2, 2),
                   self.add.s(5, 6)) | self.add.s()).delay()

    def test_destination_for(self):
        req = Mock(name='request')
        req.reply_to = 'reply_to'
        req.correlation_id = 'corid'
        assert self.b.destination_for('task_id', req) == ('reply_to', 'corid')
        task = Mock()
        _task_stack.push(task)
        try:
            task.request.reply_to = 'reply_to'
            task.request.correlation_id = 'corid'
            assert self.b.destination_for('task_id', None) == (
                'reply_to', 'corid',
            )
        finally:
            _task_stack.pop()

        with pytest.raises(RuntimeError):
            self.b.destination_for('task_id', None)

    def test_binding(self):
        queue = self.b.binding
        assert queue.name == self.b.oid
        assert queue.exchange == self.b.exchange
        assert queue.routing_key == self.b.oid
        assert queue.durable
        assert queue.auto_delete

    def test_create_binding(self):
        assert self.b._create_binding('id') == self.b.binding

    def test_on_task_call(self):
        with patch('celery.backends.rpc.maybe_declare') as md:
            with self.app.amqp.producer_pool.acquire() as prod:
                self.b.on_task_call(prod, 'task_id'),
                md.assert_called_with(
                    self.b.binding(prod.channel),
                    retry=True,
                )

    def test_create_exchange(self):
        ex = self.b._create_exchange('name')
        assert isinstance(ex, self.b.Exchange)
        assert ex.name == ''
