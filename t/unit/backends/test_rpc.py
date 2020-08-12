import pytest
from case import Mock, patch

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


class test_RPCBackend:

    def setup(self):
        self.b = RPCBackend(app=self.app)

    def test_oid(self):
        oid = self.b.oid
        oid2 = self.b.oid
        assert oid == oid2
        assert oid == self.app.oid

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
        assert not queue.durable
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
