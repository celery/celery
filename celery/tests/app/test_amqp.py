from __future__ import absolute_import, unicode_literals

from datetime import datetime, timedelta

from kombu import Exchange, Queue

from celery import uuid
from celery.app.amqp import Queues, utf8dict
from celery.five import keys
from celery.utils.timeutils import to_utc

from celery.tests.case import AppCase, Mock


class test_TaskConsumer(AppCase):

    def test_accept_content(self):
        with self.app.pool.acquire(block=True) as conn:
            self.app.conf.accept_content = ['application/json']
            self.assertEqual(
                self.app.amqp.TaskConsumer(conn).accept,
                {'application/json'},
            )
            self.assertEqual(
                self.app.amqp.TaskConsumer(conn, accept=['json']).accept,
                {'application/json'},
            )


class test_ProducerPool(AppCase):

    def test_setup_nolimit(self):
        self.app.conf.broker_pool_limit = None
        try:
            delattr(self.app, '_pool')
        except AttributeError:
            pass
        self.app.amqp._producer_pool = None
        pool = self.app.amqp.producer_pool
        self.assertEqual(pool.limit, self.app.pool.limit)
        self.assertFalse(pool._resource.queue)

        r1 = pool.acquire()
        r2 = pool.acquire()
        r1.release()
        r2.release()
        r1 = pool.acquire()
        r2 = pool.acquire()

    def test_setup(self):
        self.app.conf.broker_pool_limit = 2
        try:
            delattr(self.app, '_pool')
        except AttributeError:
            pass
        self.app.amqp._producer_pool = None
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


class test_Queues(AppCase):

    def test_queues_format(self):
        self.app.amqp.queues._consume_from = {}
        self.assertEqual(self.app.amqp.queues.format(), '')

    def test_with_defaults(self):
        self.assertEqual(Queues(None), {})

    def test_add(self):
        q = Queues()
        q.add('foo', exchange='ex', routing_key='rk')
        self.assertIn('foo', q)
        self.assertIsInstance(q['foo'], Queue)
        self.assertEqual(q['foo'].routing_key, 'rk')

    def test_with_ha_policy(self):
        qn = Queues(ha_policy=None, create_missing=False)
        qn.add('xyz')
        self.assertIsNone(qn['xyz'].queue_arguments)

        qn.add('xyx', queue_arguments={'x-foo': 'bar'})
        self.assertEqual(qn['xyx'].queue_arguments, {'x-foo': 'bar'})

        q = Queues(ha_policy='all', create_missing=False)
        q.add(Queue('foo'))
        self.assertEqual(q['foo'].queue_arguments, {'x-ha-policy': 'all'})

        qq = Queue('xyx2', queue_arguments={'x-foo': 'bari'})
        q.add(qq)
        self.assertEqual(q['xyx2'].queue_arguments, {
            'x-ha-policy': 'all',
            'x-foo': 'bari',
        })

        q2 = Queues(ha_policy=['A', 'B', 'C'], create_missing=False)
        q2.add(Queue('foo'))
        self.assertEqual(q2['foo'].queue_arguments, {
            'x-ha-policy': 'nodes',
            'x-ha-policy-params': ['A', 'B', 'C'],
        })

    def test_select_add(self):
        q = Queues()
        q.select(['foo', 'bar'])
        q.select_add('baz')
        self.assertItemsEqual(keys(q._consume_from), ['foo', 'bar', 'baz'])

    def test_deselect(self):
        q = Queues()
        q.select(['foo', 'bar'])
        q.deselect('bar')
        self.assertItemsEqual(keys(q._consume_from), ['foo'])

    def test_with_ha_policy_compat(self):
        q = Queues(ha_policy='all')
        q.add('bar')
        self.assertEqual(q['bar'].queue_arguments, {'x-ha-policy': 'all'})

    def test_add_default_exchange(self):
        ex = Exchange('fff', 'fanout')
        q = Queues(default_exchange=ex)
        q.add(Queue('foo'))
        self.assertEqual(q['foo'].exchange.name, '')

    def test_alias(self):
        q = Queues()
        q.add(Queue('foo', alias='barfoo'))
        self.assertIs(q['barfoo'], q['foo'])

    def test_with_max_priority(self):
        qs1 = Queues(max_priority=10)
        qs1.add('foo')
        self.assertEqual(qs1['foo'].queue_arguments, {'x-max-priority': 10})

        q1 = Queue('xyx', queue_arguments={'x-max-priority': 3})
        qs1.add(q1)
        self.assertEqual(qs1['xyx'].queue_arguments, {
            'x-max-priority': 3,
        })

        q1 = Queue('moo', queue_arguments=None)
        qs1.add(q1)
        self.assertEqual(qs1['moo'].queue_arguments, {
            'x-max-priority': 10,
        })

        qs2 = Queues(ha_policy='all', max_priority=5)
        qs2.add('bar')
        self.assertEqual(qs2['bar'].queue_arguments, {
            'x-ha-policy': 'all',
            'x-max-priority': 5
        })

        q2 = Queue('xyx2', queue_arguments={'x-max-priority': 2})
        qs2.add(q2)
        self.assertEqual(qs2['xyx2'].queue_arguments, {
            'x-ha-policy': 'all',
            'x-max-priority': 2,
        })

        qs3 = Queues(max_priority=None)
        qs3.add('foo2')
        self.assertEqual(qs3['foo2'].queue_arguments, None)

        q3 = Queue('xyx3', queue_arguments={'x-max-priority': 7})
        qs3.add(q3)
        self.assertEqual(qs3['xyx3'].queue_arguments, {
            'x-max-priority': 7,
        })


class test_AMQP(AppCase):

    def setup(self):
        self.simple_message = self.app.amqp.as_task_v2(
            uuid(), 'foo', create_sent_event=True,
        )

    def test_Queues__with_ha_policy(self):
        x = self.app.amqp.Queues({}, ha_policy='all')
        self.assertEqual(x.ha_policy, 'all')

    def test_Queues__with_max_priority(self):
        x = self.app.amqp.Queues({}, max_priority=23)
        self.assertEqual(x.max_priority, 23)

    def test_send_task_message__no_kwargs(self):
        self.app.amqp.send_task_message(Mock(), 'foo', self.simple_message)

    def test_send_task_message__properties(self):
        prod = Mock(name='producer')
        self.app.amqp.send_task_message(
            prod, 'foo', self.simple_message, foo=1, retry=False,
        )
        self.assertEqual(prod.publish.call_args[1]['foo'], 1)

    def test_send_task_message__headers(self):
        prod = Mock(name='producer')
        self.app.amqp.send_task_message(
            prod, 'foo', self.simple_message, headers={'x1x': 'y2x'},
            retry=False,
        )
        self.assertEqual(prod.publish.call_args[1]['headers']['x1x'], 'y2x')

    def test_send_task_message__queue_string(self):
        prod = Mock(name='producer')
        self.app.amqp.send_task_message(
            prod, 'foo', self.simple_message, queue='foo', retry=False,
        )
        kwargs = prod.publish.call_args[1]
        self.assertEqual(kwargs['routing_key'], 'foo')
        self.assertEqual(kwargs['exchange'], '')

    def test_send_event_exchange_string(self):
        evd = Mock(name='evd')
        self.app.amqp.send_task_message(
            Mock(), 'foo', self.simple_message, retry=False,
            exchange='xyz', routing_key='xyb',
            event_dispatcher=evd,
        )
        evd.publish.assert_called()
        event = evd.publish.call_args[0][1]
        self.assertEqual(event['routing_key'], 'xyb')
        self.assertEqual(event['exchange'], 'xyz')

    def test_send_task_message__with_delivery_mode(self):
        prod = Mock(name='producer')
        self.app.amqp.send_task_message(
            prod, 'foo', self.simple_message, delivery_mode=33, retry=False,
        )
        self.assertEqual(prod.publish.call_args[1]['delivery_mode'], 33)

    def test_routes(self):
        r1 = self.app.amqp.routes
        r2 = self.app.amqp.routes
        self.assertIs(r1, r2)


class test_as_task_v2(AppCase):

    def test_raises_if_args_is_not_tuple(self):
        with self.assertRaises(TypeError):
            self.app.amqp.as_task_v2(uuid(), 'foo', args='123')

    def test_raises_if_kwargs_is_not_mapping(self):
        with self.assertRaises(TypeError):
            self.app.amqp.as_task_v2(uuid(), 'foo', kwargs=(1, 2, 3))

    def test_countdown_to_eta(self):
        now = to_utc(datetime.utcnow()).astimezone(self.app.timezone)
        m = self.app.amqp.as_task_v2(
            uuid(), 'foo', countdown=10, now=now,
        )
        self.assertEqual(
            m.headers['eta'],
            (now + timedelta(seconds=10)).isoformat(),
        )

    def test_expires_to_datetime(self):
        now = to_utc(datetime.utcnow()).astimezone(self.app.timezone)
        m = self.app.amqp.as_task_v2(
            uuid(), 'foo', expires=30, now=now,
        )
        self.assertEqual(
            m.headers['expires'],
            (now + timedelta(seconds=30)).isoformat(),
        )

    def test_callbacks_errbacks_chord(self):

        @self.app.task
        def t(i):
            pass

        m = self.app.amqp.as_task_v2(
            uuid(), 'foo',
            callbacks=[t.s(1), t.s(2)],
            errbacks=[t.s(3), t.s(4)],
            chord=t.s(5),
        )
        _, _, embed = m.body
        self.assertListEqual(
            embed['callbacks'], [utf8dict(t.s(1)), utf8dict(t.s(2))],
        )
        self.assertListEqual(
            embed['errbacks'], [utf8dict(t.s(3)), utf8dict(t.s(4))],
        )
        self.assertEqual(embed['chord'], utf8dict(t.s(5)))
