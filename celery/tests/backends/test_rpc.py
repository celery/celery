from __future__ import absolute_import

from mock import patch

from celery.backends.rpc import RPCBackend
from celery._state import _task_stack

from celery.tests.case import AppCase, Mock


class test_RPCBackend(AppCase):

    def setup(self):
        self.b = RPCBackend(app=self.app)

    def test_oid(self):
        oid = self.b.oid
        oid2 = self.b.oid
        self.assertEqual(oid, oid2)
        self.assertEqual(oid, self.app.oid)

    def test_interface(self):
        self.b.on_reply_declare('task_id')

    def test_current_routing_key(self):
        task = Mock()
        _task_stack.push(task)
        try:
            task.request.reply_to = 'reply_to'
            self.assertEqual(self.b._routing_key('task_id'), 'reply_to')
        finally:
            _task_stack.pop()

    def test_binding(self):
        queue = self.b.binding
        self.assertEqual(queue.name, self.b.oid)
        self.assertEqual(queue.exchange, self.b.exchange)
        self.assertEqual(queue.routing_key, self.b.oid)
        self.assertFalse(queue.durable)
        self.assertFalse(queue.auto_delete)

    def test_many_bindings(self):
        self.assertListEqual(
            self.b._many_bindings(['a', 'b']),
            [self.b.binding],
        )

    def test_create_binding(self):
        self.assertEqual(self.b._create_binding('id'), self.b.binding)

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
        self.assertIsInstance(ex, self.b.Exchange)
        self.assertEqual(ex.name, 'c.rep')
        self.assertEqual(ex.type, 'direct')
        self.assertEqual(ex.delivery_mode, 1)
        self.assertFalse(ex.durable)
        self.assertFalse(ex.auto_delete)
