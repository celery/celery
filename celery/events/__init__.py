# -*- coding: utf-8 -*-
"""
    celery.events
    ~~~~~~~~~~~~~

    Events is a stream of messages sent for certain actions occurring
    in the worker (and clients if :setting:`CELERY_SEND_TASK_SENT_EVENT`
    is enabled), used for monitoring purposes.

"""
from __future__ import absolute_import

import os
import time
import socket
import threading

from collections import deque
from contextlib import contextmanager
from copy import copy
from operator import itemgetter

from kombu import Exchange, Queue, Producer
from kombu.mixins import ConsumerMixin
from kombu.utils import cached_property

from celery.app import app_or_default
from celery.utils import uuid
from celery.utils.timeutils import adjust_timestamp, utcoffset

event_exchange = Exchange('celeryev', type='topic')

_TZGETTER = itemgetter('utcoffset', 'timestamp')


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


def group_from(type):
    return type.split('-', 1)[0]


class EventDispatcher(object):
    """Send events as messages.

    :param connection: Connection to the broker.

    :keyword hostname: Hostname to identify ourselves as,
        by default uses the hostname returned by :func:`socket.gethostname`.

    :keyword groups: List of groups to send events for.  :meth:`send` will
        ignore send requests to groups not in this list.
        If this is :const:`None`, all events will be sent. Example groups
        include ``"task"`` and ``"worker"``.

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
                 serializer=None, groups=None):
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
        self.groups = set(groups or [])
        self.tzoffset = [-time.timezone, -time.altzone]
        self.clock = self.app.clock
        if not connection and channel:
            self.connection = channel.connection.client
        self.enabled = enabled
        if self.connection.transport.driver_type in self.DISABLED_TRANSPORTS:
            self.enabled = False
        if self.enabled:
            self.enable()
        self.headers = {'hostname': self.hostname}
        self.pid = os.getpid()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def get_exchange(self):
        if self.connection:
            return get_exchange(self.connection)
        else:
            return get_exchange(self.channel.connection.client)

    def enable(self):
        self.producer = Producer(self.channel or self.connection,
                                 exchange=self.get_exchange(),
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

    def publish(self, type, fields, producer, retry=False,
                retry_policy=None, blind=False, utcoffset=utcoffset,
                Event=Event):
        with self.mutex:
            clock = None if blind else self.clock.forward()
            event = Event(type, hostname=self.hostname, utcoffset=utcoffset(),
                          pid=self.pid, clock=clock, **fields)
            exchange = get_exchange(producer.connection)
            producer.publish(
                event,
                routing_key=type.replace('-', '.'),
                exchange=exchange.name,
                retry=retry,
                retry_policy=retry_policy,
                declare=[exchange],
                serializer=self.serializer,
                headers=self.headers,
            )

    def send(self, type, blind=False, **fields):
        """Send event.

        :param type: Kind of event.
        :keyword utcoffset: Function returning the current utcoffset in hours.
        :keyword blind: Do not send clock value
        :keyword \*\*fields: Event arguments.

        """
        if self.enabled:
            groups = self.groups
            if groups and group_from(type) not in groups:
                return
            try:
                self.publish(type, fields, self.producer, blind)
            except Exception as exc:
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


class EventReceiver(ConsumerMixin):
    """Capture events.

    :param connection: Connection to the broker.
    :keyword handlers: Event handlers.

    :attr:`handlers` is a dict of event types and their handlers,
    the special handler `"*"` captures all events that doesn't have a
    handler.

    """

    def __init__(self, connection, handlers=None, routing_key='#',
                 node_id=None, app=None, queue_prefix='celeryev'):
        self.app = app_or_default(app)
        self.connection = connection
        self.handlers = {} if handlers is None else handlers
        self.routing_key = routing_key
        self.node_id = node_id or uuid()
        self.queue_prefix = queue_prefix
        self.queue = Queue('.'.join([self.queue_prefix, self.node_id]),
                           exchange=self.get_exchange(),
                           routing_key=self.routing_key,
                           auto_delete=True,
                           durable=False)
        self.adjust_clock = self.app.clock.adjust

    def get_exchange(self):
        return get_exchange(self.connection)

    def process(self, type, event):
        """Process the received event by dispatching it to the appropriate
        handler."""
        handler = self.handlers.get(type) or self.handlers.get('*')
        handler and handler(event)

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.queue],
                         callbacks=[self._receive], no_ack=True)]

    def on_consume_ready(self, connection, channel, consumers,
                         wakeup=True, **kwargs):
        if wakeup:
            self.wakeup_workers(channel=channel)

    def itercapture(self, limit=None, timeout=None, wakeup=True):
        return self.consume(limit=limit, timeout=timeout, wakeup=wakeup)

    def capture(self, limit=None, timeout=None, wakeup=True):
        """Open up a consumer capturing events.

        This has to run in the main process, and it will never
        stop unless forced via :exc:`KeyboardInterrupt` or :exc:`SystemExit`.

        """
        return list(self.consume(limit=limit, timeout=timeout, wakeup=wakeup))

    def wakeup_workers(self, channel=None):
        self.app.control.broadcast('heartbeat',
                                   connection=self.connection,
                                   channel=channel)

    def event_from_message(self, body, localize=True, now=time.time):
        type = body.get('type', '').lower()
        clock = body.get('clock')
        if clock:
            self.adjust_clock(clock)

        if localize:
            try:
                offset, timestamp = _TZGETTER(body)
            except KeyError:
                pass
            else:
                body['timestamp'] = adjust_timestamp(timestamp, offset)
        return type, Event(type, body, local_received=now())

    def _receive(self, body, message):
        self.process(*self.event_from_message(body))


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
