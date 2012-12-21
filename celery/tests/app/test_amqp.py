from __future__ import absolute_import
from __future__ import with_statement

from kombu import Exchange, Queue
from mock import Mock

from celery.app.amqp import Queues, TaskPublisher
from celery.tests.utils import AppCase


class test_TaskProducer(AppCase):

    def test__exit__(self):
        publisher = self.app.amqp.TaskProducer(self.app.connection())
        publisher.release = Mock()
        with publisher:
            pass
        publisher.release.assert_called_with()

    def test_declare(self):
        publisher = self.app.amqp.TaskProducer(self.app.connection())
        publisher.exchange.name = 'foo'
        publisher.declare()
        publisher.exchange.name = None
        publisher.declare()

    def test_retry_policy(self):
        prod = self.app.amqp.TaskProducer(Mock())
        prod.channel.connection.client.declared_entities = set()
        prod.publish_task('tasks.add', (2, 2), {},
                          retry_policy={'frobulate': 32.4})

    def test_publish_no_retry(self):
        prod = self.app.amqp.TaskProducer(Mock())
        prod.channel.connection.client.declared_entities = set()
        prod.publish_task('tasks.add', (2, 2), {}, retry=False, chord=123)
        self.assertFalse(prod.connection.ensure.call_count)


class test_compat_TaskPublisher(AppCase):

    def test_compat_exchange_is_string(self):
        producer = TaskPublisher(exchange='foo', app=self.app)
        self.assertIsInstance(producer.exchange, Exchange)
        self.assertEqual(producer.exchange.name, 'foo')
        self.assertEqual(producer.exchange.type, 'direct')
        producer = TaskPublisher(exchange='foo', exchange_type='topic',
                                 app=self.app)
        self.assertEqual(producer.exchange.type, 'topic')

    def test_compat_exchange_is_Exchange(self):
        producer = TaskPublisher(exchange=Exchange('foo'))
        self.assertEqual(producer.exchange.name, 'foo')


class test_PublisherPool(AppCase):

    def test_setup_nolimit(self):
        L = self.app.conf.BROKER_POOL_LIMIT
        self.app.conf.BROKER_POOL_LIMIT = None
        try:
            delattr(self.app, '_pool')
        except AttributeError:
            pass
        self.app.amqp._producer_pool = None
        try:
            pool = self.app.amqp.producer_pool
            self.assertEqual(pool.limit, self.app.pool.limit)
            self.assertFalse(pool._resource.queue)

            r1 = pool.acquire()
            r2 = pool.acquire()
            r1.release()
            r2.release()
            r1 = pool.acquire()
            r2 = pool.acquire()
        finally:
            self.app.conf.BROKER_POOL_LIMIT = L

    def test_setup(self):
        L = self.app.conf.BROKER_POOL_LIMIT
        self.app.conf.BROKER_POOL_LIMIT = 2
        try:
            delattr(self.app, '_pool')
        except AttributeError:
            pass
        self.app.amqp._producer_pool = None
        try:
            pool = self.app.amqp.producer_pool
            self.assertEqual(pool.limit, self.app.pool.limit)
            self.assertTrue(pool._resource.queue)

            p1 = r1 = pool.acquire()
            p2 = r2 = pool.acquire()
            r1.release()
            r2.release()
            r1 = pool.acquire()
            r2 = pool.acquire()
            self.assertIs(p2, r1)
            self.assertIs(p1, r2)
            r1.release()
            r2.release()
        finally:
            self.app.conf.BROKER_POOL_LIMIT = L


class test_Queues(AppCase):

    def test_queues_format(self):
        prev, self.app.amqp.queues._consume_from = (
            self.app.amqp.queues._consume_from, {})
        try:
            self.assertEqual(self.app.amqp.queues.format(), '')
        finally:
            self.app.amqp.queues._consume_from = prev

    def test_with_defaults(self):
        self.assertEqual(Queues(None), {})

    def test_add(self):
        q = Queues()
        q.add('foo', exchange='ex', routing_key='rk')
        self.assertIn('foo', q)
        self.assertIsInstance(q['foo'], Queue)
        self.assertEqual(q['foo'].routing_key, 'rk')

    def test_add_default_exchange(self):
        ex = Exchange('fff', 'fanout')
        q = Queues(default_exchange=ex)
        q.add(Queue('foo'))
        self.assertEqual(q['foo'].exchange, ex)

    def test_alias(self):
        q = Queues()
        q.add(Queue('foo', alias='barfoo'))
        self.assertIs(q['barfoo'], q['foo'])
