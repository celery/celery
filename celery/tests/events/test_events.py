from __future__ import absolute_import, unicode_literals

import socket

from celery.events import CLIENT_CLOCK_SKEW, Event

from celery.tests.case import AppCase, Mock, call


class MockProducer(object):

    raise_on_publish = False

    def __init__(self, *args, **kwargs):
        self.sent = []

    def publish(self, msg, *args, **kwargs):
        if self.raise_on_publish:
            raise KeyError()
        self.sent.append(msg)

    def close(self):
        pass

    def has_event(self, kind):
        for event in self.sent:
            if event['type'] == kind:
                return event
        return False


class test_Event(AppCase):

    def test_constructor(self):
        event = Event('world war II')
        self.assertEqual(event['type'], 'world war II')
        self.assertTrue(event['timestamp'])


class test_EventDispatcher(AppCase):

    def test_redis_uses_fanout_exchange(self):
        self.app.connection = Mock()
        conn = self.app.connection.return_value = Mock()
        conn.transport.driver_type = 'redis'

        dispatcher = self.app.events.Dispatcher(conn, enabled=False)
        self.assertEqual(dispatcher.exchange.type, 'fanout')

    def test_others_use_topic_exchange(self):
        self.app.connection = Mock()
        conn = self.app.connection.return_value = Mock()
        conn.transport.driver_type = 'amqp'
        dispatcher = self.app.events.Dispatcher(conn, enabled=False)
        self.assertEqual(dispatcher.exchange.type, 'topic')

    def test_takes_channel_connection(self):
        x = self.app.events.Dispatcher(channel=Mock())
        self.assertIs(x.connection, x.channel.connection.client)

    def test_sql_transports_disabled(self):
        conn = Mock()
        conn.transport.driver_type = 'sql'
        x = self.app.events.Dispatcher(connection=conn)
        self.assertFalse(x.enabled)

    def test_send(self):
        producer = MockProducer()
        producer.connection = self.app.connection_for_write()
        connection = Mock()
        connection.transport.driver_type = 'amqp'
        eventer = self.app.events.Dispatcher(connection, enabled=False,
                                             buffer_while_offline=False)
        eventer.producer = producer
        eventer.enabled = True
        eventer.send('World War II', ended=True)
        self.assertTrue(producer.has_event('World War II'))
        eventer.enabled = False
        eventer.send('World War III')
        self.assertFalse(producer.has_event('World War III'))

        evs = ('Event 1', 'Event 2', 'Event 3')
        eventer.enabled = True
        eventer.producer.raise_on_publish = True
        eventer.buffer_while_offline = False
        with self.assertRaises(KeyError):
            eventer.send('Event X')
        eventer.buffer_while_offline = True
        for ev in evs:
            eventer.send(ev)
        eventer.producer.raise_on_publish = False
        eventer.flush()
        for ev in evs:
            self.assertTrue(producer.has_event(ev))

        eventer.flush()

    def test_send_buffer_group(self):
        buf_received = [None]
        producer = MockProducer()
        producer.connection = self.app.connection_for_write()
        connection = Mock()
        connection.transport.driver_type = 'amqp'
        eventer = self.app.events.Dispatcher(
            connection, enabled=False,
            buffer_group={'task'}, buffer_limit=2,
        )
        eventer.producer = producer
        eventer.enabled = True
        eventer._publish = Mock(name='_publish')

        def on_eventer_publish(events, *args, **kwargs):
            buf_received[0] = list(events)
        eventer._publish.side_effect = on_eventer_publish
        self.assertFalse(eventer._group_buffer['task'])
        eventer.on_send_buffered = Mock(name='on_send_buffered')
        eventer.send('task-received', uuid=1)
        prev_buffer = eventer._group_buffer['task']
        self.assertTrue(eventer._group_buffer['task'])
        eventer.on_send_buffered.assert_called_with()
        eventer.send('task-received', uuid=1)
        self.assertFalse(eventer._group_buffer['task'])
        eventer._publish.assert_has_calls([
            call([], eventer.producer, 'task.multi'),
        ])
        # clear in place
        self.assertIs(eventer._group_buffer['task'], prev_buffer)
        self.assertEqual(len(buf_received[0]), 2)
        eventer.on_send_buffered = None
        eventer.send('task-received', uuid=1)

    def test_flush_no_groups_no_errors(self):
        eventer = self.app.events.Dispatcher(Mock())
        eventer.flush(errors=False, groups=False)

    def test_enter_exit(self):
        with self.app.connection_for_write() as conn:
            d = self.app.events.Dispatcher(conn)
            d.close = Mock()
            with d as _d:
                self.assertTrue(_d)
            d.close.assert_called_with()

    def test_enable_disable_callbacks(self):
        on_enable = Mock()
        on_disable = Mock()
        with self.app.connection_for_write() as conn:
            with self.app.events.Dispatcher(conn, enabled=False) as d:
                d.on_enabled.add(on_enable)
                d.on_disabled.add(on_disable)
                d.enable()
                on_enable.assert_called_with()
                d.disable()
                on_disable.assert_called_with()

    def test_enabled_disable(self):
        connection = self.app.connection_for_write()
        channel = connection.channel()
        try:
            dispatcher = self.app.events.Dispatcher(connection,
                                                    enabled=True)
            dispatcher2 = self.app.events.Dispatcher(connection,
                                                     enabled=True,
                                                     channel=channel)
            self.assertTrue(dispatcher.enabled)
            self.assertTrue(dispatcher.producer.channel)
            self.assertEqual(dispatcher.producer.serializer,
                             self.app.conf.event_serializer)

            created_channel = dispatcher.producer.channel
            dispatcher.disable()
            dispatcher.disable()  # Disable with no active producer
            dispatcher2.disable()
            self.assertFalse(dispatcher.enabled)
            self.assertIsNone(dispatcher.producer)
            self.assertFalse(dispatcher2.channel.closed,
                             'does not close manually provided channel')

            dispatcher.enable()
            self.assertTrue(dispatcher.enabled)
            self.assertTrue(dispatcher.producer)

            # XXX test compat attribute
            self.assertIs(dispatcher.publisher, dispatcher.producer)
            prev, dispatcher.publisher = dispatcher.producer, 42
            try:
                self.assertEqual(dispatcher.producer, 42)
            finally:
                dispatcher.producer = prev
        finally:
            channel.close()
            connection.close()
        self.assertTrue(created_channel.closed)


class test_EventReceiver(AppCase):

    def test_process(self):

        message = {'type': 'world-war'}

        got_event = [False]

        def my_handler(event):
            got_event[0] = True

        connection = Mock()
        connection.transport_cls = 'memory'
        r = self.app.events.Receiver(
            connection,
            handlers={'world-war': my_handler},
            node_id='celery.tests',
        )
        r._receive(message, object())
        self.assertTrue(got_event[0])

    def test_accept_argument(self):
        r = self.app.events.Receiver(Mock(), accept={'app/foo'})
        self.assertEqual(r.accept, {'app/foo'})

    def test_event_queue_prefix__default(self):
        r = self.app.events.Receiver(Mock())
        self.assertTrue(r.queue.name.startswith('celeryev.'))

    def test_event_queue_prefix__setting(self):
        self.app.conf.event_queue_prefix = 'eventq'
        r = self.app.events.Receiver(Mock())
        self.assertTrue(r.queue.name.startswith('eventq.'))

    def test_event_queue_prefix__argument(self):
        r = self.app.events.Receiver(Mock(), queue_prefix='fooq')
        self.assertTrue(r.queue.name.startswith('fooq.'))

    def test_catch_all_event(self):

        message = {'type': 'world-war'}

        got_event = [False]

        def my_handler(event):
            got_event[0] = True

        connection = Mock()
        connection.transport_cls = 'memory'
        r = self.app.events.Receiver(connection, node_id='celery.tests')
        r.handlers['*'] = my_handler
        r._receive(message, object())
        self.assertTrue(got_event[0])

    def test_itercapture(self):
        connection = self.app.connection_for_write()
        try:
            r = self.app.events.Receiver(connection, node_id='celery.tests')
            it = r.itercapture(timeout=0.0001, wakeup=False)

            with self.assertRaises(socket.timeout):
                next(it)

            with self.assertRaises(socket.timeout):
                r.capture(timeout=0.00001)
        finally:
            connection.close()

    def test_event_from_message_localize_disabled(self):
        r = self.app.events.Receiver(Mock(), node_id='celery.tests')
        r.adjust_clock = Mock()
        ts_adjust = Mock()

        r.event_from_message(
            {'type': 'worker-online', 'clock': 313},
            localize=False,
            adjust_timestamp=ts_adjust,
        )
        ts_adjust.assert_not_called()
        r.adjust_clock.assert_called_with(313)

    def test_event_from_message_clock_from_client(self):
        r = self.app.events.Receiver(Mock(), node_id='celery.tests')
        r.clock.value = 302
        r.adjust_clock = Mock()

        body = {'type': 'task-sent'}
        r.event_from_message(
            body, localize=False, adjust_timestamp=Mock(),
        )
        self.assertEqual(body['clock'], r.clock.value + CLIENT_CLOCK_SKEW)

    def test_receive_multi(self):
        r = self.app.events.Receiver(Mock(name='connection'))
        r.process = Mock(name='process')
        efm = r.event_from_message = Mock(name='event_from_message')

        def on_efm(*args):
            return args
        efm.side_effect = on_efm
        r._receive([1, 2, 3], Mock())
        r.process.assert_has_calls([call(1), call(2), call(3)])

    def test_itercapture_limit(self):
        connection = self.app.connection_for_write()
        channel = connection.channel()
        try:
            events_received = [0]

            def handler(event):
                events_received[0] += 1

            producer = self.app.events.Dispatcher(
                connection, enabled=True, channel=channel,
            )
            r = self.app.events.Receiver(
                connection,
                handlers={'*': handler},
                node_id='celery.tests',
            )
            evs = ['ev1', 'ev2', 'ev3', 'ev4', 'ev5']
            for ev in evs:
                producer.send(ev)
            it = r.itercapture(limit=4, wakeup=True)
            next(it)  # skip consumer (see itercapture)
            list(it)
            self.assertEqual(events_received[0], 4)
        finally:
            channel.close()
            connection.close()


class test_misc(AppCase):

    def test_State(self):
        state = self.app.events.State()
        self.assertDictEqual(dict(state.workers), {})

    def test_default_dispatcher(self):
        with self.app.events.default_dispatcher() as d:
            self.assertTrue(d)
            self.assertTrue(d.connection)
