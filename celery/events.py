from celery.messaging import EventPublisher, EventConsumer
from UserDict import UserDict
import time

"""
Events
======

WORKER-ONLINE    hostname timestamp
WORKER-OFFLINE   hostname timestamp
TASK-RECEIVED    id name args kwargs retries eta queue exchange rkey timestamp
TASK-ACCEPTED    id timestamp
TASK-SUCCEEDED   id result timestamp
TASK-FAILED      id exception timestamp
TASK-RETRIED     id exception timestamp
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
        self.publisher.send(Event(type, **fields))


class EventReceiver(object):
    handlers = {}

    def __init__(self, connection, handlers=None):
        self.connection = connection
        if handlers is not None:
            self.handlers = handlers

    def process(self, event):
        type = event["type"]
        handler = self.handlers.get(type) or self.handlers.get("*")
        handler and handler(event)

    def _receive(message, message_data):
        self.process(message_data)

    def consume(self, limit=None):
        consumer = EventConsumer(self.connection)
        consumer.register_callback(self._receive)
        it = consumer.iterconsume(limit=limit)
        while True:
            it.next()
