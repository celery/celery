from celery.messaging import EventPublisher, EventConsumer
from UserDict import UserDict
import time

"""
Events
======

WORKER-ONLINE    hostname timestamp
WORKER-OFFLINE   hostname timestamp
TASK-RECEIVED    uuid name args kwargs retries eta timestamp
TASK-ACCEPTED    uuid timestamp
TASK-SUCCEEDED   uuid result timestamp
TASK-FAILED      uuid exception timestamp
TASK-RETRIED     uuid exception timestamp
WORKER-HEARTBEAT hostname timestamp

"""

def Event(type, **fields):
    return dict(fields, type=type, timestamp=time.time())


class EventDispatcher(object):
    """

    dispatcher.send("worker-heartbeat", hostname="h8.opera.com")

    """
    def __init__(self, connection):
        self.connection = connection
        self.publisher = EventPublisher(self.connection)

    def send(self, type, **fields):
        fields["timestamp"] = time.time()
        self.publisher.send(Event(type, **fields))


class EventReceiver(object):
    handlers = {}

    def __init__(self, connection, handlers=None):
        self.connection = connection
        if handlers is not None:
            self.handlers = handlers

    def process(self, type, event):
        print("Received event: %s" % event)
        handler = self.handlers.get(type) or self.handlers.get("*")
        handler and handler(event)

    def _receive(self, message_data, message):
        type = message_data.pop("type").lower()
        self.process(type, Event(type, **message_data))

    def consume(self, limit=None):
        consumer = EventConsumer(self.connection)
        consumer.register_callback(self._receive)
        it = consumer.iterconsume(limit=limit)
        while True:
            it.next()
