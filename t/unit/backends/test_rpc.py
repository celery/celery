import socket
import uuid
from unittest.mock import Mock, patch

import pytest

from celery import chord, group, states
from celery._state import _task_stack
from celery.app.task import Context
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

    def test_drain_events_socket_timeout_does_not_trigger_reconnect(self):
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

    def test_to_result_extended(self):
        self.app.conf.result_extended = True
        request = Context(
            task='mytask', args=[1, 2], kwargs={'foo': 'bar'},
            hostname='worker1@example.com', retries=2,
            delivery_info={'routing_key': 'celery'},
        )
        meta = self.b._to_result('task-id', states.SUCCESS, 42, None, request)
        assert meta['task_id'] == 'task-id'
        assert meta['status'] == states.SUCCESS
        assert meta['result'] == 42
        assert meta['traceback'] is None
        assert meta['name'] == 'mytask'
        assert meta['args'] == [1, 2]
        assert meta['kwargs'] == {'foo': 'bar'}
        assert meta['worker'] == 'worker1@example.com'
        assert meta['retries'] == 2
        assert meta['queue'] == 'celery'

    def test_to_result_not_extended(self):
        self.app.conf.result_extended = False
        request = Context(
            task='mytask', args=[1, 2], kwargs={'foo': 'bar'},
            delivery_info={'routing_key': 'celery'},
        )
        meta = self.b._to_result('task-id', states.SUCCESS, 42, None, request)
        assert meta['task_id'] == 'task-id'
        assert meta['status'] == states.SUCCESS
        assert meta['result'] == 42
        assert meta['traceback'] is None
        for key in ('name', 'args', 'kwargs', 'worker', 'retries', 'queue'):
            assert key not in meta


class test_RPCBackend_result_lifecycle:
    """Result message lifecycle: polling must not leak or recirculate."""

    def setup_method(self):
        self.b = RPCBackend(app=self.app)

    def make_message(self, task_id, status, result=None):
        message = Mock(name=f'message-{task_id}')
        message.payload = {
            'task_id': task_id,
            'status': status,
            'result': result,
            'traceback': None,
        }
        message.properties = {'correlation_id': task_id}
        return message

    def slurp(self, messages):
        return patch.object(
            self.b, '_slurp_from_queue', return_value=iter(messages))

    def test_final_state_is_acked_not_requeued(self):
        message = self.make_message('tid1', states.SUCCESS, 42)
        with self.slurp([message]):
            meta = self.b.get_task_meta('tid1')
        assert meta['status'] == states.SUCCESS
        assert meta['result'] == 42
        message.ack.assert_called_once_with()
        message.requeue.assert_not_called()

    def test_final_state_cached_when_cache_enabled(self):
        # the test app sets result_cache_max=-1 (cache disabled),
        # with caching on the final meta is served from the cache.
        old_cache_max = self.app.conf.result_cache_max
        self.app.conf.result_cache_max = 100
        try:
            b = RPCBackend(app=self.app)
            message = self.make_message('tid1', states.SUCCESS, 42)
            with patch.object(b, '_slurp_from_queue',
                              return_value=iter([message])):
                b.get_task_meta('tid1')
            assert b._cache['tid1']['status'] == states.SUCCESS
            with patch.object(b, '_slurp_from_queue',
                              return_value=iter([])):
                assert b.get_task_meta('tid1')['result'] == 42
        finally:
            self.app.conf.result_cache_max = old_cache_max

    def test_non_final_state_is_requeued(self):
        message = self.make_message('tid1', states.RETRY)
        with self.slurp([message]):
            meta = self.b.get_task_meta('tid1')
        assert meta['status'] == states.RETRY
        message.requeue.assert_called_once_with()
        message.ack.assert_not_called()

    def test_final_state_resolves_pending_waiter(self):
        # a waiter blocked in get() must still resolve when a poll
        # consumes the final message first.
        result = self.app.AsyncResult('tid1')
        self.b._add_pending_result('tid1', result)
        message = self.make_message('tid1', states.SUCCESS, 42)
        with self.slurp([message]):
            self.b.get_task_meta('tid1')
        message.ack.assert_called_once_with()
        assert result._cache['status'] == states.SUCCESS
        assert result._cache['result'] == 42

    def test_final_state_buffered_for_later_waiter(self):
        # a get() started after the poll resolves from the buffer
        # instead of hanging on a message that will never arrive.
        message = self.make_message('tid1', states.SUCCESS, 42)
        with self.slurp([message]):
            self.b.get_task_meta('tid1')
        buffered = self.b._pending_messages.take('tid1')
        assert buffered['status'] == states.SUCCESS

    def test_repeated_final_polls_do_not_recirculate(self):
        # regression test for #4830: polling many completed tasks must
        # leave no messages on the queue and nothing in _out_of_band.
        messages = [
            self.make_message(f'tid{i}', states.SUCCESS, i)
            for i in range(10)
        ]
        for i, message in enumerate(messages):
            with self.slurp([message]):
                meta = self.b.get_task_meta(f'tid{i}')
            assert meta['status'] == states.SUCCESS
            message.ack.assert_called_once_with()
        assert self.b._out_of_band == {}
        # later polls are served from the cache, no queue traffic needed.
        with self.slurp([]):
            assert self.b.get_task_meta('tid3')['result'] == 3

    def test_out_of_band_final_state_is_cached_and_acked(self):
        message = self.make_message('other', states.SUCCESS, 7)
        with self.slurp([message]):
            meta = self.b.get_task_meta('tid1')
        assert meta['status'] == states.PENDING
        message.ack.assert_called_once_with()
        assert 'other' not in self.b._out_of_band
        # a later poll for that task still sees the final state,
        # served from the pending buffer without queue traffic.
        with self.slurp([]):
            assert self.b.get_task_meta('other')['result'] == 7

    def test_out_of_band_non_final_state_is_buffered_and_acked(self):
        message = self.make_message('other', states.STARTED)
        with self.slurp([message]):
            self.b.get_task_meta('tid1')
        message.ack.assert_called_once_with()
        assert self.b._out_of_band['other'] is message
        # a later poll for that task uses the buffered copy.
        with self.slurp([]):
            meta = self.b.get_task_meta('other')
        assert meta['status'] == states.STARTED
        assert 'other' not in self.b._out_of_band

    def test_stale_buffered_state_dropped_on_consumer_final(self):
        # a stale non-final entry buffered by an earlier poll must not
        # regress the state once the consumer delivers the final meta.
        stale = self.make_message('tid1', states.STARTED)
        self.b._out_of_band['tid1'] = stale
        final = self.make_message('tid1', states.SUCCESS, 42)
        self.b.result_consumer.on_state_change(final.payload, final)
        assert 'tid1' not in self.b._out_of_band
        assert self.b._pending_messages.take('tid1')['status'] == (
            states.SUCCESS)

    def test_forget_clears_out_of_band_and_cache(self):
        message = self.make_message('tid1', states.STARTED)
        self.b._out_of_band['tid1'] = message
        self.b._cache['tid1'] = {'status': states.STARTED}
        self.b.forget('tid1')
        assert 'tid1' not in self.b._out_of_band
        assert 'tid1' not in self.b._cache

    def test_forget_does_not_raise(self):
        # RPCBackend inherited the base _forget which just raises.
        self.b.forget('tid1')

    def test_after_fork_clears_out_of_band(self):
        self.b._out_of_band['tid1'] = self.make_message(
            'tid1', states.STARTED)
        self.b._after_fork()
        assert self.b._out_of_band == {}

    def test_forget_clears_pending_messages(self):
        # forget() must drop the buffered final state too, otherwise a
        # later poll would still resolve the task as completed.
        message = self.make_message('tid1', states.SUCCESS, 42)
        with self.slurp([message]):
            self.b.get_task_meta('tid1')
        assert self.b._pending_messages.get('tid1')
        self.b.forget('tid1')
        assert self.b._pending_messages.get('tid1') is None
        assert self.b._pending_messages.total == 0
        with self.slurp([]):
            assert self.b.get_task_meta('tid1')['status'] == states.PENDING

    def test_after_fork_clears_pending_messages_and_cache(self):
        # the forked child must not inherit the parent's buffered
        # final states or cached metas.
        message = self.make_message('tid1', states.SUCCESS, 42)
        with self.slurp([message]):
            self.b.get_task_meta('tid1')
        self.b._cache = {'tid1': {'status': states.SUCCESS, 'result': 42}}
        self.b._after_fork()
        assert self.b._pending_messages.get('tid1') is None
        assert self.b._pending_messages.total == 0
        assert self.b._cache == {}
