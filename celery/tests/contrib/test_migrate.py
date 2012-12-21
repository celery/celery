from __future__ import absolute_import
from __future__ import with_statement

from kombu import Connection, Producer, Queue, Exchange
from kombu.exceptions import StdChannelError
from mock import patch

from celery.contrib.migrate import (
    State,
    migrate_task,
    migrate_tasks,
)
from celery.utils.encoding import bytes_t, ensure_bytes
from celery.tests.utils import AppCase, Case, Mock


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
            'properties': {}
        },
    )


class test_State(Case):

    def test_strtotal(self):
        x = State()
        self.assertEqual(x.strtotal, u'?')
        x.total_apx = 100
        self.assertEqual(x.strtotal, u'100')


class test_migrate_task(Case):

    def test_removes_compression_header(self):
        x = Message('foo', compression='zlib')
        producer = Mock()
        migrate_task(producer, x.body, x)
        self.assertTrue(producer.publish.called)
        args, kwargs = producer.publish.call_args
        self.assertIsInstance(args[0], bytes_t)
        self.assertNotIn('compression', kwargs['headers'])
        self.assertEqual(kwargs['compression'], 'zlib')
        self.assertEqual(kwargs['content_type'], 'application/json')
        self.assertEqual(kwargs['content_encoding'], 'utf-8')
        self.assertEqual(kwargs['exchange'], 'exchange')
        self.assertEqual(kwargs['routing_key'], 'rkey')


class test_migrate_tasks(AppCase):

    def test_migrate(self, name='testcelery'):
        x = Connection('memory://foo')
        y = Connection('memory://foo')
        # use separate state
        x.default_channel.queues = {}
        y.default_channel.queues = {}

        ex = Exchange(name, 'direct')
        q = Queue(name, exchange=ex, routing_key=name)
        q(x.default_channel).declare()
        Producer(x).publish('foo', exchange=name, routing_key=name)
        Producer(x).publish('bar', exchange=name, routing_key=name)
        Producer(x).publish('baz', exchange=name, routing_key=name)
        self.assertTrue(x.default_channel.queues)
        self.assertFalse(y.default_channel.queues)

        migrate_tasks(x, y)

        yq = q(y.default_channel)
        self.assertEqual(yq.get().body, ensure_bytes('foo'))
        self.assertEqual(yq.get().body, ensure_bytes('bar'))
        self.assertEqual(yq.get().body, ensure_bytes('baz'))

        Producer(x).publish('foo', exchange=name, routing_key=name)
        callback = Mock()
        migrate_tasks(x, y, callback=callback)
        self.assertTrue(callback.called)
        migrate = Mock()
        Producer(x).publish('baz', exchange=name, routing_key=name)
        migrate_tasks(x, y, callback=callback, migrate=migrate)
        self.assertTrue(migrate.called)

        with patch('kombu.transport.virtual.Channel.queue_declare') as qd:

            def effect(*args, **kwargs):
                if kwargs.get('passive'):
                    raise StdChannelError()
                return 0, 3, 0
            qd.side_effect = effect
            migrate_tasks(x, y)

        x = Connection('memory://')
        x.default_channel.queues = {}
        y.default_channel.queues = {}
        callback = Mock()
        migrate_tasks(x, y, callback=callback)
        self.assertFalse(callback.called)
