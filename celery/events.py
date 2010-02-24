import time
import socket
import threading

from celery.messaging import EventPublisher, EventConsumer


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

    :keyword enabled: Set to ``False`` to not actually publish any events,
        making :meth:`send` a noop operation.

    You need to :meth:`close` this after use.

    """

    def __init__(self, connection, hostname=None, enabled=True,
            publisher=None):
        self.connection = connection
        self.publisher = publisher or EventPublisher(self.connection)
        self.hostname = hostname or socket.gethostname()
        self.enabled = enabled
        self._lock = threading.Lock()

    def send(self, type, **fields):
        """Send event.

        :param type: Kind of event.
        :keyword \*\*fields: Event arguments.

        """
        if not self.enabled:
            return

        self._lock.acquire()
        did_exc = None
        try:
            self.publisher.send(Event(type, hostname=self.hostname))
        except Exception, e:
            did_exc = e

        self._lock.release()
        
        if did_exc:
            raise did_exc

    def close(self):
        """Close the event dispatcher."""
        self._lock.locked() and self._lock.release()
        self.publisher and self.publisher.close()


class EventReceiver(object):
    """Capture events.

    :param connection: Carrot connection.
    :keyword handlers: Event handlers.

    :attr:`handlers`` is a dict of event types and their handlers,
    the special handler ``"*`"`` captures all events that doesn't have a
    handler.

    """
    handlers = {}

    def __init__(self, connection, handlers=None):
        self.connection = connection
        if handlers is not None:
            self.handlers = handlers

    def process(self, type, event):
        """Process the received event by dispatching it to the appropriate
        handler."""
        handler = self.handlers.get(type) or self.handlers.get("*")
        handler and handler(event)

    def capture(self, limit=None):
        """Open up a consumer capturing events.

        This has to run in the main process, and it will never
        stop unless forced via :exc:`KeyboardInterrupt` or :exc:`SystemExit`.

        """
        consumer = EventConsumer(self.connection)
        consumer.register_callback(self._receive)
        it = consumer.iterconsume(limit=limit)
        while True:
            it.next()

    def _receive(self, message_data, message):
        type = message_data.pop("type").lower()
        self.process(type, create_event(type, message_data))
