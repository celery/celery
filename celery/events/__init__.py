# -*- coding: utf-8 -*-
"""
    celery.events
    ~~~~~~~~~~~~~

    Events are messages sent for actions happening
    in the worker (and clients if :setting:`CELERY_SEND_TASK_SENT_EVENT`
    is enabled), used for monitoring purposes.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import time
import socket
import threading

from collections import deque
from contextlib import contextmanager
from itertools import count

from kombu.entity import Exchange, Queue
from kombu.messaging import Consumer, Producer

from ..app import app_or_default
from ..utils import uuid

event_exchange = Exchange("celeryev", type="topic")


def Event(type, _fields=None, **fields):
    """Create an event.

    An event is a dictionary, the only required field is ``type``.

    """
    event = dict(_fields or {}, type=type, **fields)
    if "timestamp" not in event:
        event["timestamp"] = time.time()
    return event


class EventDispatcher(object):
    """Send events as messages.

    :param connection: Connection to the broker.

    :keyword hostname: Hostname to identify ourselves as,
        by default uses the hostname returned by :func:`socket.gethostname`.

    :keyword enabled: Set to :const:`False` to not actually publish any events,
        making :meth:`send` a noop operation.

    :keyword channel: Can be used instead of `connection` to specify
        an exact channel to use when sending events.

    :keyword buffer_while_offline: If enabled events will be buffered
       while the connection is down. :meth:`flush` must be called
       as soon as the connection is re-established.

    You need to :meth:`close` this after use.

    """

    def __init__(self, connection=None, hostname=None, enabled=True,
            channel=None, buffer_while_offline=True, app=None,
            serializer=None):
        self.app = app_or_default(app)
        self.connection = connection
        self.channel = channel
        self.hostname = hostname or socket.gethostname()
        self.buffer_while_offline = buffer_while_offline
        self.mutex = threading.Lock()
        self.publisher = None
        self._outbound_buffer = deque()
        self.serializer = serializer or self.app.conf.CELERY_EVENT_SERIALIZER

        self.enabled = enabled
        if self.enabled:
            self.enable()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def enable(self):
        self.publisher = Producer(self.channel or self.connection.channel(),
                                  exchange=event_exchange,
                                  serializer=self.serializer)
        self.enabled = True

    def disable(self):
        if self.enabled:
            self.enabled = False
            self.close()

    def send(self, type, **fields):
        """Send event.

        :param type: Kind of event.
        :keyword \*\*fields: Event arguments.

        """
        if self.enabled:
            with self.mutex:
                event = Event(type, hostname=self.hostname,
                                    clock=self.app.clock.forward(), **fields)
                try:
                    self.publisher.publish(event,
                                           routing_key=type.replace("-", "."))
                except Exception, exc:
                    if not self.buffer_while_offline:
                        raise
                    self._outbound_buffer.append((type, fields, exc))

    def flush(self):
        while self._outbound_buffer:
            try:
                type, fields, _ = self._outbound_buffer.popleft()
            except IndexError:
                return
            self.send(type, **fields)

    def copy_buffer(self, other):
        self._outbound_buffer = other._outbound_buffer

    def close(self):
        """Close the event dispatcher."""
        self.mutex.locked() and self.mutex.release()
        if self.publisher is not None:
            if not self.channel:  # close auto channel.
                self.publisher.channel.close()
            self.publisher = None


class EventReceiver(object):
    """Capture events.

    :param connection: Connection to the broker.
    :keyword handlers: Event handlers.

    :attr:`handlers` is a dict of event types and their handlers,
    the special handler `"*"` captures all events that doesn't have a
    handler.

    """
    handlers = {}

    def __init__(self, connection, handlers=None, routing_key="#",
            node_id=None, app=None):
        self.app = app_or_default(app)
        self.connection = connection
        if handlers is not None:
            self.handlers = handlers
        self.routing_key = routing_key
        self.node_id = node_id or uuid()
        self.queue = Queue("%s.%s" % ("celeryev", self.node_id),
                           exchange=event_exchange,
                           routing_key=self.routing_key,
                           auto_delete=True,
                           durable=False)

    def process(self, type, event):
        """Process the received event by dispatching it to the appropriate
        handler."""
        handler = self.handlers.get(type) or self.handlers.get("*")
        handler and handler(event)

    @contextmanager
    def consumer(self):
        """Create event consumer.

        .. warning::

            This creates a new channel that needs to be closed
            by calling `consumer.channel.close()`.

        """
        consumer = Consumer(self.connection.channel(),
                            queues=[self.queue], no_ack=True)
        consumer.register_callback(self._receive)
        with consumer:
            yield consumer
        consumer.channel.close()

    def itercapture(self, limit=None, timeout=None, wakeup=True):
        with self.consumer() as consumer:
            if wakeup:
                self.wakeup_workers(channel=consumer.channel)

            yield consumer

            self.drain_events(limit=limit, timeout=timeout)

    def capture(self, limit=None, timeout=None, wakeup=True):
        """Open up a consumer capturing events.

        This has to run in the main process, and it will never
        stop unless forced via :exc:`KeyboardInterrupt` or :exc:`SystemExit`.

        """
        list(self.itercapture(limit=limit, timeout=timeout, wakeup=wakeup))

    def wakeup_workers(self, channel=None):
        self.app.control.broadcast("heartbeat",
                                   connection=self.connection,
                                   channel=channel)

    def drain_events(self, limit=None, timeout=None):
        for iteration in count(0):
            if limit and iteration >= limit:
                break
            try:
                self.connection.drain_events(timeout=timeout)
            except socket.timeout:
                if timeout:
                    raise
            except socket.error:
                pass

    def _receive(self, body, message):
        type = body.pop("type").lower()
        clock = body.get("clock")
        if clock:
            self.app.clock.adjust(clock)
        self.process(type, Event(type, body))


class Events(object):

    def __init__(self, app=None):
        self.app = app

    def Receiver(self, connection, handlers=None, routing_key="#",
            node_id=None):
        return EventReceiver(connection,
                             handlers=handlers,
                             routing_key=routing_key,
                             node_id=node_id,
                             app=self.app)

    def Dispatcher(self, connection=None, hostname=None, enabled=True,
            channel=None, buffer_while_offline=True):
        return EventDispatcher(connection,
                               hostname=hostname,
                               enabled=enabled,
                               channel=channel,
                               app=self.app)

    def State(self):
        from .state import State as _State
        return _State()

    @contextmanager
    def default_dispatcher(self, hostname=None, enabled=True,
            buffer_while_offline=False):
        with self.app.amqp.publisher_pool.acquire(block=True) as pub:
            with self.Dispatcher(pub.connection, hostname, enabled,
                                 pub.channel, buffer_while_offline) as d:
                yield d
