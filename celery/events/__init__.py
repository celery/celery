# -*- coding: utf-8 -*-
"""
    celery.events
    ~~~~~~~~~~~~~

    Events is a stream of messages sent for certain actions occurring
    in the worker (and clients if :setting:`CELERY_SEND_TASK_SENT_EVENT`
    is enabled), used for monitoring purposes.

"""
from __future__ import absolute_import
from __future__ import with_statement

import time
import socket
import threading

from collections import deque
from contextlib import contextmanager
from copy import copy

from kombu import eventloop, Exchange, Queue, Consumer, Producer
from kombu.utils import cached_property

from celery.app import app_or_default
from celery.utils import uuid

event_exchange = Exchange('celeryev', type='topic')


def get_exchange(conn):
    ex = copy(event_exchange)
    if conn.transport.driver_type == 'redis':
        # quick hack for Issue #436
        ex.type = 'fanout'
    return ex


def Event(type, _fields=None, **fields):
    """Create an event.

    An event is a dictionary, the only required field is ``type``.

    """
    event = dict(_fields or {}, type=type, **fields)
    if 'timestamp' not in event:
        event['timestamp'] = time.time()
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
    DISABLED_TRANSPORTS = set(['sql'])

    def __init__(self, connection=None, hostname=None, enabled=True,
                 channel=None, buffer_while_offline=True, app=None,
                 serializer=None):
        self.app = app_or_default(app or self.app)
        self.connection = connection
        self.channel = channel
        self.hostname = hostname or socket.gethostname()
        self.buffer_while_offline = buffer_while_offline
        self.mutex = threading.Lock()
        self.producer = None
        self._outbound_buffer = deque()
        self.serializer = serializer or self.app.conf.CELERY_EVENT_SERIALIZER
        self.on_enabled = set()
        self.on_disabled = set()

        self.enabled = enabled
        if not connection and channel:
            self.connection = channel.connection.client
        self.enabled = enabled
        conninfo = self.connection or self.app.connection()
        self.exchange = get_exchange(conninfo)
        if conninfo.transport.driver_type in self.DISABLED_TRANSPORTS:
            self.enabled = False
        if self.enabled:
            self.enable()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def enable(self):
        self.producer = Producer(self.channel or self.connection,
                                 exchange=self.exchange,
                                 serializer=self.serializer)
        self.enabled = True
        for callback in self.on_enabled:
            callback()

    def disable(self):
        if self.enabled:
            self.enabled = False
            self.close()
            for callback in self.on_disabled:
                callback()

    def publish(self, type, fields, producer, retry=False, retry_policy=None):
        with self.mutex:
            event = Event(type, hostname=self.hostname,
                          clock=self.app.clock.forward(), **fields)
            exchange = self.exchange
            producer.publish(
                event,
                routing_key=type.replace('-', '.'),
                exchange=exchange.name,
                retry=retry,
                retry_policy=retry_policy,
                declare=[exchange],
                serializer=self.serializer,
            )

    def send(self, type, **fields):
        """Send event.

        :param type: Kind of event.
        :keyword \*\*fields: Event arguments.

        """
        if self.enabled:
            try:
                self.publish(type, fields, self.producer)
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
        self.producer = None

    def _get_publisher(self):
        return self.producer

    def _set_publisher(self, producer):
        self.producer = producer
    publisher = property(_get_publisher, _set_publisher)  # XXX compat


class EventReceiver(object):
    """Capture events.

    :param connection: Connection to the broker.
    :keyword handlers: Event handlers.

    :attr:`handlers` is a dict of event types and their handlers,
    the special handler `"*"` captures all events that doesn't have a
    handler.

    """
    handlers = {}

    def __init__(self, connection, handlers=None, routing_key='#',
                 node_id=None, app=None, queue_prefix='celeryev'):
        self.app = app_or_default(app)
        self.connection = connection
        if handlers is not None:
            self.handlers = handlers
        self.routing_key = routing_key
        self.node_id = node_id or uuid()
        self.queue_prefix = queue_prefix
        self.exchange = get_exchange(self.connection or self.app.connection())
        self.queue = Queue('.'.join([self.queue_prefix, self.node_id]),
                           exchange=self.exchange,
                           routing_key=self.routing_key,
                           auto_delete=True,
                           durable=False)

    def process(self, type, event):
        """Process the received event by dispatching it to the appropriate
        handler."""
        handler = self.handlers.get(type) or self.handlers.get('*')
        handler and handler(event)

    @contextmanager
    def consumer(self, wakeup=True):
        """Create event consumer."""
        consumer = Consumer(self.connection,
                            queues=[self.queue], no_ack=True,
                            accept=['application/json'])
        consumer.register_callback(self._receive)
        consumer.consume()

        try:
            if wakeup:
                self.wakeup_workers(channel=consumer.channel)
            yield consumer
        finally:
            try:
                consumer.cancel()
            except self.connection.connection_errors:
                pass

    def itercapture(self, limit=None, timeout=None, wakeup=True):
        with self.consumer(wakeup=wakeup) as consumer:
            yield consumer
            self.drain_events(limit=limit, timeout=timeout)

    def capture(self, limit=None, timeout=None, wakeup=True):
        """Open up a consumer capturing events.

        This has to run in the main process, and it will never
        stop unless forced via :exc:`KeyboardInterrupt` or :exc:`SystemExit`.

        """
        list(self.itercapture(limit=limit, timeout=timeout, wakeup=wakeup))

    def wakeup_workers(self, channel=None):
        self.app.control.broadcast('heartbeat',
                                   connection=self.connection,
                                   channel=channel)

    def drain_events(self, **kwargs):
        for _ in eventloop(self.connection, **kwargs):
            pass

    def _receive(self, body, message):
        type = body.pop('type').lower()
        clock = body.get('clock')
        if clock:
            self.app.clock.adjust(clock)
        self.process(type, Event(type, body))


class Events(object):

    def __init__(self, app=None):
        self.app = app

    @cached_property
    def Receiver(self):
        return self.app.subclass_with_self(EventReceiver,
                                           reverse='events.Receiver')

    @cached_property
    def Dispatcher(self):
        return self.app.subclass_with_self(EventDispatcher,
                                           reverse='events.Dispatcher')

    @cached_property
    def State(self):
        return self.app.subclass_with_self('celery.events.state:State',
                                           reverse='events.State')

    @contextmanager
    def default_dispatcher(self, hostname=None, enabled=True,
                           buffer_while_offline=False):
        with self.app.amqp.producer_pool.acquire(block=True) as pub:
            with self.Dispatcher(pub.connection, hostname, enabled,
                                 pub.channel, buffer_while_offline) as d:
                yield d
