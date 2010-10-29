import unittest2 as unittest

from celery import events


class MockProducer(object):

    def __init__(self, *args, **kwargs):
        self.sent = []

    def publish(self, msg, *args, **kwargs):
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

    def test_send(self):
        producer = MockProducer()
        eventer = events.EventDispatcher(object(), enabled=False)
        eventer.publisher = producer
        eventer.enabled = True
        eventer.send("World War II", ended=True)
        self.assertTrue(producer.has_event("World War II"))


class TestEventReceiver(unittest.TestCase):

    def test_process(self):

        message = {"type": "world-war"}

        got_event = [False]

        def my_handler(event):
            got_event[0] = True

        r = events.EventReceiver(object(), handlers={
                                    "world-war": my_handler})
        r._receive(message, object())
        self.assertTrue(got_event[0])

    def test_catch_all_event(self):

        message = {"type": "world-war"}

        got_event = [False]

        def my_handler(event):
            got_event[0] = True

        r = events.EventReceiver(object())
        events.EventReceiver.handlers["*"] = my_handler
        try:
            r._receive(message, object())
            self.assertTrue(got_event[0])
        finally:
            events.EventReceiver.handlers = {}
