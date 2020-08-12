from contextlib import contextmanager

import pytest
from amqp import ChannelError
from case import Mock, mock, patch
from kombu import Connection, Exchange, Producer, Queue
from kombu.transport.virtual import QoS

from celery.contrib.migrate import (State, StopFiltering, _maybe_queue,
                                    expand_dest, filter_callback,
                                    filter_status, migrate_task,
                                    migrate_tasks, move, move_by_idmap,
                                    move_by_taskmap, move_task_by_id,
                                    start_filter, task_id_eq, task_id_in)
from celery.utils.encoding import ensure_bytes

# hack to ignore error at shutdown
QoS.restore_at_shutdown = False


def Message(body, exchange='exchange', routing_key='rkey',
            compression=None, content_type='application/json',
            content_encoding='utf-8'):
    return Mock(
        attrs={
            'body': body,
            'delivery_info': {
                'exchange': exchange,
                'routing_key': routing_key,
            },
            'headers': {
                'compression': compression,
            },
            'content_type': content_type,
            'content_encoding': content_encoding,
            'properties': {
                'correlation_id': isinstance(body, dict) and body['id'] or None
            }
        },
    )


class test_State:

    def test_strtotal(self):
        x = State()
        assert x.strtotal == '?'
        x.total_apx = 100
        assert x.strtotal == '100'

    def test_repr(self):
        x = State()
        assert repr(x)
        x.filtered = 'foo'
        assert repr(x)


class test_move:

    @contextmanager
    def move_context(self, **kwargs):
        with patch('celery.contrib.migrate.start_filter') as start:
            with patch('celery.contrib.migrate.republish') as republish:
                pred = Mock(name='predicate')
                move(pred, app=self.app,
                     connection=self.app.connection(), **kwargs)
                start.assert_called()
                callback = start.call_args[0][2]
                yield callback, pred, republish

    def msgpair(self, **kwargs):
        body = dict({'task': 'add', 'id': 'id'}, **kwargs)
        return body, Message(body)

    def test_move(self):
        with self.move_context() as (callback, pred, republish):
            pred.return_value = None
            body, message = self.msgpair()
            callback(body, message)
            message.ack.assert_not_called()
            republish.assert_not_called()

            pred.return_value = 'foo'
            callback(body, message)
            message.ack.assert_called_with()
            republish.assert_called()

    def test_move_transform(self):
        trans = Mock(name='transform')
        trans.return_value = Queue('bar')
        with self.move_context(transform=trans) as (callback, pred, republish):
            pred.return_value = 'foo'
            body, message = self.msgpair()
            with patch('celery.contrib.migrate.maybe_declare') as maybed:
                callback(body, message)
                trans.assert_called_with('foo')
                maybed.assert_called()
                republish.assert_called()

    def test_limit(self):
        with self.move_context(limit=1) as (callback, pred, republish):
            pred.return_value = 'foo'
            body, message = self.msgpair()
            with pytest.raises(StopFiltering):
                callback(body, message)
            republish.assert_called()

    def test_callback(self):
        cb = Mock()
        with self.move_context(callback=cb) as (callback, pred, republish):
            pred.return_value = 'foo'
            body, message = self.msgpair()
            callback(body, message)
            republish.assert_called()
            cb.assert_called()


class test_start_filter:

    def test_start(self):
        with patch('celery.contrib.migrate.eventloop') as evloop:
            app = Mock()
            filt = Mock(name='filter')
            conn = Connection('memory://')
            evloop.side_effect = StopFiltering()
            app.amqp.queues = {'foo': Queue('foo'), 'bar': Queue('bar')}
            consumer = app.amqp.TaskConsumer.return_value = Mock(name='consum')
            consumer.queues = list(app.amqp.queues.values())
            consumer.channel = conn.default_channel
            consumer.__enter__ = Mock(name='consumer.__enter__')
            consumer.__exit__ = Mock(name='consumer.__exit__')
            consumer.callbacks = []

            def register_callback(x):
                consumer.callbacks.append(x)
            consumer.register_callback = register_callback

            start_filter(app, conn, filt,
                         queues='foo,bar', ack_messages=True)
            body = {'task': 'add', 'id': 'id'}
            for callback in consumer.callbacks:
                callback(body, Message(body))
            consumer.callbacks[:] = []
            cb = Mock(name='callback=')
            start_filter(app, conn, filt, tasks='add,mul', callback=cb)
            for callback in consumer.callbacks:
                callback(body, Message(body))
            cb.assert_called()

            on_declare_queue = Mock()
            start_filter(app, conn, filt, tasks='add,mul', queues='foo',
                         on_declare_queue=on_declare_queue)
            on_declare_queue.assert_called()
            start_filter(app, conn, filt, queues=['foo', 'bar'])
            consumer.callbacks[:] = []
            state = State()
            start_filter(app, conn, filt,
                         tasks='add,mul', callback=cb, state=state, limit=1)
            stop_filtering_raised = False
            for callback in consumer.callbacks:
                try:
                    callback(body, Message(body))
                except StopFiltering:
                    stop_filtering_raised = True
            assert state.count
            assert stop_filtering_raised


class test_filter_callback:

    def test_filter(self):
        callback = Mock()
        filt = filter_callback(callback, ['add', 'mul'])
        t1 = {'task': 'add'}
        t2 = {'task': 'div'}

        message = Mock()
        filt(t2, message)
        callback.assert_not_called()
        filt(t1, message)
        callback.assert_called_with(t1, message)


def test_task_id_in():
    assert task_id_in(['A'], {'id': 'A'}, Mock())
    assert not task_id_in(['A'], {'id': 'B'}, Mock())


def test_task_id_eq():
    assert task_id_eq('A', {'id': 'A'}, Mock())
    assert not task_id_eq('A', {'id': 'B'}, Mock())


def test_expand_dest():
    assert expand_dest(None, 'foo', 'bar') == ('foo', 'bar')
    assert expand_dest(('b', 'x'), 'foo', 'bar') == ('b', 'x')


def test_maybe_queue():
    app = Mock()
    app.amqp.queues = {'foo': 313}
    assert _maybe_queue(app, 'foo') == 313
    assert _maybe_queue(app, Queue('foo')) == Queue('foo')


def test_filter_status():
    with mock.stdouts() as (stdout, stderr):
        filter_status(State(), {'id': '1', 'task': 'add'}, Mock())
        assert stdout.getvalue()


def test_move_by_taskmap():
    with patch('celery.contrib.migrate.move') as move:
        move_by_taskmap({'add': Queue('foo')})
        move.assert_called()
        cb = move.call_args[0][0]
        assert cb({'task': 'add'}, Mock())


def test_move_by_idmap():
    with patch('celery.contrib.migrate.move') as move:
        move_by_idmap({'123f': Queue('foo')})
        move.assert_called()
        cb = move.call_args[0][0]
        body = {'id': '123f'}
        assert cb(body, Message(body))


def test_move_task_by_id():
    with patch('celery.contrib.migrate.move') as move:
        move_task_by_id('123f', Queue('foo'))
        move.assert_called()
        cb = move.call_args[0][0]
        body = {'id': '123f'}
        assert cb(body, Message(body)) == Queue('foo')


class test_migrate_task:

    def test_removes_compression_header(self):
        x = Message('foo', compression='zlib')
        producer = Mock()
        migrate_task(producer, x.body, x)
        producer.publish.assert_called()
        args, kwargs = producer.publish.call_args
        assert isinstance(args[0], bytes)
        assert 'compression' not in kwargs['headers']
        assert kwargs['compression'] == 'zlib'
        assert kwargs['content_type'] == 'application/json'
        assert kwargs['content_encoding'] == 'utf-8'
        assert kwargs['exchange'] == 'exchange'
        assert kwargs['routing_key'] == 'rkey'


class test_migrate_tasks:

    def test_migrate(self, app, name='testcelery'):
        connection_kwargs = {
            'transport_options': {'polling_interval': 0.01}
        }
        x = Connection('memory://foo', **connection_kwargs)
        y = Connection('memory://foo', **connection_kwargs)
        # use separate state
        x.default_channel.queues = {}
        y.default_channel.queues = {}

        ex = Exchange(name, 'direct')
        q = Queue(name, exchange=ex, routing_key=name)
        q(x.default_channel).declare()
        Producer(x).publish('foo', exchange=name, routing_key=name)
        Producer(x).publish('bar', exchange=name, routing_key=name)
        Producer(x).publish('baz', exchange=name, routing_key=name)
        assert x.default_channel.queues
        assert not y.default_channel.queues
        migrate_tasks(x, y, accept=['text/plain'], app=app)

        yq = q(y.default_channel)
        assert yq.get().body == ensure_bytes('foo')
        assert yq.get().body == ensure_bytes('bar')
        assert yq.get().body == ensure_bytes('baz')

        Producer(x).publish('foo', exchange=name, routing_key=name)
        callback = Mock()
        migrate_tasks(x, y,
                      callback=callback, accept=['text/plain'], app=app)
        callback.assert_called()
        migrate = Mock()
        Producer(x).publish('baz', exchange=name, routing_key=name)
        migrate_tasks(x, y, callback=callback,
                      migrate=migrate, accept=['text/plain'], app=app)
        migrate.assert_called()

        with patch('kombu.transport.virtual.Channel.queue_declare') as qd:

            def effect(*args, **kwargs):
                if kwargs.get('passive'):
                    raise ChannelError('some channel error')
                return 0, 3, 0
            qd.side_effect = effect
            migrate_tasks(x, y, app=app)

        x = Connection('memory://', **connection_kwargs)
        x.default_channel.queues = {}
        y.default_channel.queues = {}
        callback = Mock()
        migrate_tasks(x, y,
                      callback=callback, accept=['text/plain'], app=app)
        callback.assert_not_called()
