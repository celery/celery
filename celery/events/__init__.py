import time
import socket
import threading

from collections import deque
from itertools import count

from kombu.compat import Publisher, Consumer

from celery.app import app_or_default


def create_event(type, fields):
    std = {"type": type,
           "timestamp": fields.get("timestamp") or time.time()}
    return dict(fields, **std)


def Event(type, **fields):
    """Create an event.

    An event is a dictionary, the only required field is the type.

    """
    return create_event(type, fields)


class EventDispatcher(object):
    """Send events as messages.

    :param connection: Carrot connection.

    :keyword hostname: Hostname to identify ourselves as,
        by default uses the hostname returned by :func:`socket.gethostname`.

    :keyword enabled: Set to :const:`False` to not actually publish any events,
        making :meth:`send` a noop operation.

    You need to :meth:`close` this after use.

    """

    def __init__(self, connection, hostname=None, enabled=True, app=None):
        self.app = app_or_default(app)
        self.connection = connection
        self.hostname = hostname or socket.gethostname()
        self.enabled = enabled
        self._lock = threading.Lock()
        self.publisher = None
        self._outbound_buffer = deque()

        if self.enabled:
            self.enable()

    def enable(self):
        conf = self.app.conf
        self.enabled = True
        self.publisher = Publisher(self.connection,
                                exchange=conf.CELERY_EVENT_EXCHANGE,
                                exchange_type=conf.CELERY_EVENT_EXCHANGE_TYPE,
                                routing_key=conf.CELERY_EVENT_ROUTING_KEY,
                                serializer=conf.CELERY_EVENT_SERIALIZER)

    def disable(self):
        self.enabled = False
        if self.publisher is not None:
            self.publisher.close()
            self.publisher = None

    def send(self, type, **fields):
        """Send event.

        :param type: Kind of event.
        :keyword \*\*fields: Event arguments.

        """
        if not self.enabled:
            return

        self._lock.acquire()
        event = Event(type, hostname=self.hostname, **fields)
        try:
            try:
                self.publisher.send(event)
            except Exception, exc:
                self._outbound_buffer.append((event, exc))
        finally:
            self._lock.release()

    def flush(self):
        while self._outbound_buffer:
            event, _ = self._outbound_buffer.popleft()
            self.publisher.send(event)

    def close(self):
        """Close the event dispatcher."""
        self._lock.locked() and self._lock.release()
        self.publisher and self.publisher.close()


class EventReceiver(object):
    """Capture events.

    :param connection: Carrot connection.
    :keyword handlers: Event handlers.

    :attr:`handlers` is a dict of event types and their handlers,
    the special handler `"*"` captures all events that doesn't have a
    handler.

    """
    handlers = {}

    def __init__(self, connection, handlers=None, app=None):
        self.app = app_or_default(app)
        self.connection = connection
        if handlers is not None:
            self.handlers = handlers

    def process(self, type, event):
        """Process the received event by dispatching it to the appropriate
        handler."""
        handler = self.handlers.get(type) or self.handlers.get("*")
        handler and handler(event)

    def consumer(self):
        conf = self.app.conf
        consumer = Consumer(self.connection,
                            queue=conf.CELERY_EVENT_QUEUE,
                            exchange=conf.CELERY_EVENT_EXCHANGE,
                            exchange_type=conf.CELERY_EVENT_EXCHANGE_TYPE,
                            routing_key=conf.CELERY_EVENT_ROUTING_KEY,
                            no_ack=True)
        consumer.register_callback(self._receive)
        return consumer

    def capture(self, limit=None, timeout=None):
        """Open up a consumer capturing events.

        This has to run in the main process, and it will never
        stop unless forced via :exc:`KeyboardInterrupt` or :exc:`SystemExit`.

        """
        consumer = self.consumer()
        consumer.consume()
        try:
            for iteration in count(0):
                if limit and iteration > limit:
                    break
                try:
                    consumer.connection.drain_events(timeout=timeout)
                except socket.timeout:
                    if timeout:
                        raise
                except socket.error:
                    pass
        finally:
            consumer.close()

    def _receive(self, message_data, message):
        type = message_data.pop("type").lower()
        self.process(type, create_event(type, message_data))
