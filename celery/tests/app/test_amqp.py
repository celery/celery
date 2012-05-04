from __future__ import absolute_import
from __future__ import with_statement

from mock import Mock

from celery.tests.utils import AppCase


class test_TaskPublisher(AppCase):

    def test__exit__(self):

        publisher = self.app.amqp.TaskPublisher(self.app.broker_connection())
        publisher.release = Mock()
        with publisher:
            pass
        publisher.release.assert_called_with()

    def test_declare(self):
        publisher = self.app.amqp.TaskPublisher(self.app.broker_connection())
        publisher.exchange.name = "foo"
        publisher.declare()
        publisher.exchange.name = None
        publisher.declare()

    def test_retry_policy(self):
        pub = self.app.amqp.TaskPublisher(Mock())
        pub.channel.connection.client.declared_entities = set()
        pub.delay_task("tasks.add", (2, 2), {},
                       retry_policy={"frobulate": 32.4})

    def test_publish_no_retry(self):
        pub = self.app.amqp.TaskPublisher(Mock())
        pub.channel.connection.client.declared_entities = set()
        pub.delay_task("tasks.add", (2, 2), {}, retry=False, chord=123)
        self.assertFalse(pub.connection.ensure.call_count)


class test_PublisherPool(AppCase):

    def test_setup_nolimit(self):
        L = self.app.conf.BROKER_POOL_LIMIT
        self.app.conf.BROKER_POOL_LIMIT = None
        try:
            delattr(self.app, "_pool")
        except AttributeError:
            pass
        self.app.amqp.__dict__.pop("publisher_pool", None)
        try:
            pool = self.app.amqp.publisher_pool
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
            delattr(self.app, "_pool")
        except AttributeError:
            pass
        self.app.amqp.__dict__.pop("publisher_pool", None)
        try:
            pool = self.app.amqp.publisher_pool
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
        prev, self.app.amqp.queues._consume_from = \
                self.app.amqp.queues._consume_from, {}
        try:
            self.assertEqual(self.app.amqp.queues.format(), "")
        finally:
            self.app.amqp.queues._consume_from = prev

    def test_with_defaults(self):
        self.assertEqual(
            self.app.amqp.queues.with_defaults(None,
                self.app.amqp.default_exchange), {})
