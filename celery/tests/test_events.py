import socket

from celery import events
from celery.app import app_or_default
from celery.tests.utils import unittest


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
            if event["type"] == kind:
                return event
        return False


class TestEvent(unittest.TestCase):

    def test_constructor(self):
        event = events.Event("world war II")
        self.assertEqual(event["type"], "world war II")
        self.assertTrue(event["timestamp"])


class TestEventDispatcher(unittest.TestCase):

    def setUp(self):
        self.app = app_or_default()

    def test_send(self):
        producer = MockProducer()
        eventer = self.app.events.Dispatcher(object(), enabled=False)
        eventer.publisher = producer
        eventer.enabled = True
        eventer.send("World War II", ended=True)
        self.assertTrue(producer.has_event("World War II"))
        eventer.enabled = False
        eventer.send("World War III")
        self.assertFalse(producer.has_event("World War III"))

        evs = ("Event 1", "Event 2", "Event 3")
        eventer.enabled = True
        eventer.publisher.raise_on_publish = True
        eventer.buffer_while_offline = False
        self.assertRaises(KeyError, eventer.send, "Event X")
        eventer.buffer_while_offline = True
        for ev in evs:
            eventer.send(ev)
        eventer.publisher.raise_on_publish = False
        eventer.flush()
        for ev in evs:
            self.assertTrue(producer.has_event(ev))

    def test_enabled_disable(self):
        connection = self.app.broker_connection()
        channel = connection.channel()
        try:
            dispatcher = self.app.events.Dispatcher(connection,
                                                    enabled=True)
            dispatcher2 = self.app.events.Dispatcher(connection,
                                                     enabled=True,
                                                      channel=channel)
            self.assertTrue(dispatcher.enabled)
            self.assertTrue(dispatcher.publisher.channel)
            self.assertEqual(dispatcher.publisher.serializer,
                            self.app.conf.CELERY_EVENT_SERIALIZER)

            created_channel = dispatcher.publisher.channel
            dispatcher.disable()
            dispatcher.disable()  # Disable with no active publisher
            dispatcher2.disable()
            self.assertFalse(dispatcher.enabled)
            self.assertIsNone(dispatcher.publisher)
            self.assertTrue(created_channel.closed)
            self.assertFalse(dispatcher2.channel.closed,
                             "does not close manually provided channel")

            dispatcher.enable()
            self.assertTrue(dispatcher.enabled)
            self.assertTrue(dispatcher.publisher)
        finally:
            channel.close()
            connection.close()


class TestEventReceiver(unittest.TestCase):

    def setUp(self):
        self.app = app_or_default()

    def test_process(self):

        message = {"type": "world-war"}

        got_event = [False]

        def my_handler(event):
            got_event[0] = True

        r = events.EventReceiver(object(),
                                 handlers={"world-war": my_handler},
                                 node_id="celery.tests",
                                 )
        r._receive(message, object())
        self.assertTrue(got_event[0])

    def test_catch_all_event(self):

        message = {"type": "world-war"}

        got_event = [False]

        def my_handler(event):
            got_event[0] = True

        r = events.EventReceiver(object(), node_id="celery.tests")
        events.EventReceiver.handlers["*"] = my_handler
        try:
            r._receive(message, object())
            self.assertTrue(got_event[0])
        finally:
            events.EventReceiver.handlers = {}

    def test_itercapture(self):
        connection = self.app.broker_connection()
        try:
            r = self.app.events.Receiver(connection, node_id="celery.tests")
            it = r.itercapture(timeout=0.0001, wakeup=False)
            consumer = it.next()
            self.assertTrue(consumer.queues)
            self.assertEqual(consumer.callbacks[0], r._receive)

            self.assertRaises(socket.timeout, it.next)

            self.assertRaises(socket.timeout,
                              r.capture, timeout=0.00001)
        finally:
            connection.close()

    def test_itercapture_limit(self):
        connection = self.app.broker_connection()
        channel = connection.channel()
        try:
            events_received = [0]

            def handler(event):
                events_received[0] += 1

            producer = self.app.events.Dispatcher(connection,
                                                  enabled=True,
                                                  channel=channel)
            r = self.app.events.Receiver(connection,
                                         handlers={"*": handler},
                                         node_id="celery.tests")
            evs = ["ev1", "ev2", "ev3", "ev4", "ev5"]
            for ev in evs:
                producer.send(ev)
            it = r.itercapture(limit=4, wakeup=True)
            it.next()  # skip consumer (see itercapture)
            list(it)
            self.assertEqual(events_received[0], 4)
        finally:
            channel.close()
            connection.close()


class test_misc(unittest.TestCase):

    def setUp(self):
        self.app = app_or_default()

    def test_State(self):
        state = self.app.events.State()
        self.assertDictEqual(dict(state.workers), {})
