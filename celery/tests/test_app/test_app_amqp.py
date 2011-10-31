from __future__ import absolute_import
from __future__ import with_statement

from mock import Mock

from celery.app.amqp import MSG_OPTIONS, extract_msg_options
from celery.tests.utils import AppCase


class TestMsgOptions(AppCase):

    def test_MSG_OPTIONS(self):
        self.assertTrue(MSG_OPTIONS)

    def test_extract_msg_options(self):
        testing = {"mandatory": True, "routing_key": "foo.xuzzy"}
        result = extract_msg_options(testing)
        self.assertEqual(result["mandatory"], True)
        self.assertEqual(result["routing_key"], "foo.xuzzy")


class test_TaskPublisher(AppCase):

    def test__exit__(self):

        publisher = self.app.amqp.TaskPublisher(self.app.broker_connection())
        publisher.release = Mock()
        with publisher:
            pass
        publisher.release.assert_called_with()

    def test_ensure_declare_queue(self, q="x1242112"):
        publisher = self.app.amqp.TaskPublisher(Mock())
        self.app.amqp.queues.add(q, q, q)
        publisher._declare_queue(q, retry=True)
        self.assertTrue(publisher.connection.ensure.call_count)

    def test_ensure_declare_exchange(self, e="x9248311"):
        publisher = self.app.amqp.TaskPublisher(Mock())
        publisher._declare_exchange(e, "direct", retry=True)
        self.assertTrue(publisher.connection.ensure.call_count)

    def test_retry_policy(self):
        pub = self.app.amqp.TaskPublisher(Mock())
        pub.delay_task("tasks.add", (2, 2), {},
                       retry_policy={"frobulate": 32.4})

    def test_publish_no_retry(self):
        pub = self.app.amqp.TaskPublisher(Mock())
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
            delattr(r1.connection, "_producer_chan")
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
