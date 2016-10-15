# -*- coding: utf-8 -*-
"""Monitoring Event Receiver+Dispatcher.

Events is a stream of messages sent for certain actions occurring
in the worker (and clients if :setting:`task_send_sent_event`
is enabled), used for monitoring purposes.
"""
from __future__ import absolute_import, unicode_literals

import os
import time
import threading

from collections import defaultdict, deque
from contextlib import contextmanager
from copy import copy
from operator import itemgetter

from kombu import Exchange, Queue, Producer
from kombu.connection import maybe_channel
from kombu.mixins import ConsumerMixin
from kombu.utils.objects import cached_property

from celery import uuid
from celery.app import app_or_default
from celery.five import items
from celery.utils.nodenames import anon_nodename
from celery.utils.time import adjust_timestamp, utcoffset

__all__ = ['Events', 'Event', 'EventDispatcher', 'EventReceiver']

# pylint: disable=redefined-outer-name
# We cache globals and attribute lookups, so disable this warning.

event_exchange = Exchange('celeryev', type='topic')

_TZGETTER = itemgetter('utcoffset', 'timestamp')

CLIENT_CLOCK_SKEW = -1


def get_exchange(conn):
    ex = copy(event_exchange)
    if conn.transport.driver_type == 'redis':
        # quick hack for Issue #436
        ex.type = 'fanout'
    return ex


def Event(type, _fields=None, __dict__=dict, __now__=time.time, **fields):
    """Create an event.

    An event is a dictionary, the only required field is ``type``.
    A ``timestamp`` field will be set to the current time if not provided.
    """
    event = __dict__(_fields, **fields) if _fields else fields
    if 'timestamp' not in event:
        event.update(timestamp=__now__(), type=type)
    else:
        event['type'] = type
    return event


def group_from(type):
    """Get the group part of an event type name.

    Example:
        >>> group_from('task-sent')
        'task'

        >>> group_from('custom-my-event')
        'custom'
    """
    return type.split('-', 1)[0]


class EventDispatcher(object):
    """Dispatches event messages.

    Arguments:
        connection (kombu.Connection): Connection to the broker.

        hostname (str): Hostname to identify ourselves as,
            by default uses the hostname returned by
            :func:`~celery.utils.anon_nodename`.

        groups (Sequence[str]): List of groups to send events for.
            :meth:`send` will ignore send requests to groups not in this list.
            If this is :const:`None`, all events will be sent.
            Example groups include ``"task"`` and ``"worker"``.

        enabled (bool): Set to :const:`False` to not actually publish any
            events, making :meth:`send` a no-op.

        channel (kombu.Channel): Can be used instead of `connection` to specify
            an exact channel to use when sending events.

        buffer_while_offline (bool): If enabled events will be buffered
            while the connection is down. :meth:`flush` must be called
            as soon as the connection is re-established.

    Note:
        You need to :meth:`close` this after use.
    """

    DISABLED_TRANSPORTS = {'sql'}

    app = None

    # set of callbacks to be called when :meth:`enabled`.
    on_enabled = None

    # set of callbacks to be called when :meth:`disabled`.
    on_disabled = None

    def __init__(self, connection=None, hostname=None, enabled=True,
                 channel=None, buffer_while_offline=True, app=None,
                 serializer=None, groups=None, delivery_mode=1,
                 buffer_group=None, buffer_limit=24, on_send_buffered=None):
        self.app = app_or_default(app or self.app)
        self.connection = connection
        self.channel = channel
        self.hostname = hostname or anon_nodename()
        self.buffer_while_offline = buffer_while_offline
        self.buffer_group = buffer_group or frozenset()
        self.buffer_limit = buffer_limit
        self.on_send_buffered = on_send_buffered
        self._group_buffer = defaultdict(list)
        self.mutex = threading.Lock()
        self.producer = None
        self._outbound_buffer = deque()
        self.serializer = serializer or self.app.conf.event_serializer
        self.on_enabled = set()
        self.on_disabled = set()
        self.groups = set(groups or [])
        self.tzoffset = [-time.timezone, -time.altzone]
        self.clock = self.app.clock
        self.delivery_mode = delivery_mode
        if not connection and channel:
            self.connection = channel.connection.client
        self.enabled = enabled
        conninfo = self.connection or self.app.connection_for_write()
        self.exchange = get_exchange(conninfo)
        if conninfo.transport.driver_type in self.DISABLED_TRANSPORTS:
            self.enabled = False
        if self.enabled:
            self.enable()
        self.headers = {'hostname': self.hostname}
        self.pid = os.getpid()

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

    def publish(self, type, fields, producer,
                blind=False, Event=Event, **kwargs):
        """Publish event using custom :class:`~kombu.Producer`.

        Arguments:
            type (str): Event type name, with group separated by dash (`-`).
                fields: Dictionary of event fields, must be json serializable.
            producer (kombu.Producer): Producer instance to use:
                only the ``publish`` method will be called.
            retry (bool): Retry in the event of connection failure.
            retry_policy (Mapping): Map of custom retry policy options.
                See :meth:`~kombu.Connection.ensure`.
            blind (bool): Don't set logical clock value (also don't forward
                the internal logical clock).
            Event (Callable): Event type used to create event.
                Defaults to :func:`Event`.
            utcoffset (Callable): Function returning the current
                utc offset in hours.
        """
        clock = None if blind else self.clock.forward()
        event = Event(type, hostname=self.hostname, utcoffset=utcoffset(),
                      pid=self.pid, clock=clock, **fields)
        with self.mutex:
            return self._publish(event, producer,
                                 routing_key=type.replace('-', '.'), **kwargs)

    def _publish(self, event, producer, routing_key, retry=False,
                 retry_policy=None, utcoffset=utcoffset):
        exchange = self.exchange
        try:
            producer.publish(
                event,
                routing_key=routing_key,
                exchange=exchange.name,
                retry=retry,
                retry_policy=retry_policy,
                declare=[exchange],
                serializer=self.serializer,
                headers=self.headers,
                delivery_mode=self.delivery_mode,
            )
        except Exception as exc:  # pylint: disable=broad-except
            if not self.buffer_while_offline:
                raise
            self._outbound_buffer.append((event, routing_key, exc))

    def send(self, type, blind=False, utcoffset=utcoffset, retry=False,
             retry_policy=None, Event=Event, **fields):
        """Send event.

        Arguments:
            type (str): Event type name, with group separated by dash (`-`).
            retry (bool): Retry in the event of connection failure.
            retry_policy (Mapping): Map of custom retry policy options.
                See :meth:`~kombu.Connection.ensure`.
            blind (bool): Don't set logical clock value (also don't forward
                the internal logical clock).
            Event (Callable): Event type used to create event,
                defaults to :func:`Event`.
            utcoffset (Callable): unction returning the current utc offset
                in hours.
            **fields (Any): Event fields -- must be json serializable.
        """
        if self.enabled:
            groups, group = self.groups, group_from(type)
            if groups and group not in groups:
                return
            if group in self.buffer_group:
                clock = self.clock.forward()
                event = Event(type, hostname=self.hostname,
                              utcoffset=utcoffset(),
                              pid=self.pid, clock=clock, **fields)
                buf = self._group_buffer[group]
                buf.append(event)
                if len(buf) >= self.buffer_limit:
                    self.flush()
                elif self.on_send_buffered:
                    self.on_send_buffered()
            else:
                return self.publish(type, fields, self.producer, blind=blind,
                                    Event=Event, retry=retry,
                                    retry_policy=retry_policy)

    def flush(self, errors=True, groups=True):
        """Flush the outbound buffer."""
        if errors:
            buf = list(self._outbound_buffer)
            try:
                with self.mutex:
                    for event, routing_key, _ in buf:
                        self._publish(event, self.producer, routing_key)
            finally:
                self._outbound_buffer.clear()
        if groups:
            with self.mutex:
                for group, events in items(self._group_buffer):
                    self._publish(events, self.producer, '%s.multi' % group)
                    events[:] = []  # list.clear

    def extend_buffer(self, other):
        """Copy the outbound buffer of another instance."""
        self._outbound_buffer.extend(other._outbound_buffer)

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

    Arguments:
        connection (kombu.Connection): Connection to the broker.
        handlers (Mapping[Callable]): Event handlers.
            This is  a map of event type names and their handlers.
            The special handler `"*"` captures all events that don't have a
            handler.
    """

    app = None

    def __init__(self, channel, handlers=None, routing_key='#',
                 node_id=None, app=None, queue_prefix=None,
                 accept=None, queue_ttl=None, queue_expires=None):
        self.app = app_or_default(app or self.app)
        self.channel = maybe_channel(channel)
        self.handlers = {} if handlers is None else handlers
        self.routing_key = routing_key
        self.node_id = node_id or uuid()
        self.queue_prefix = queue_prefix or self.app.conf.event_queue_prefix
        self.exchange = get_exchange(
            self.connection or self.app.connection_for_write())
        if queue_ttl is None:
            queue_ttl = self.app.conf.event_queue_ttl
        if queue_expires is None:
            queue_expires = self.app.conf.event_queue_expires
        self.queue = Queue(
            '.'.join([self.queue_prefix, self.node_id]),
            exchange=self.exchange,
            routing_key=self.routing_key,
            auto_delete=True, durable=False,
            message_ttl=queue_ttl,
            expires=queue_expires,
        )
        self.clock = self.app.clock
        self.adjust_clock = self.clock.adjust
        self.forward_clock = self.clock.forward
        if accept is None:
            accept = {self.app.conf.event_serializer, 'json'}
        self.accept = accept

    def process(self, type, event):
        """Process event by dispatching to configured handler."""
        handler = self.handlers.get(type) or self.handlers.get('*')
        handler and handler(event)

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.queue],
                         callbacks=[self._receive], no_ack=True,
                         accept=self.accept)]

    def on_consume_ready(self, connection, channel, consumers,
                         wakeup=True, **kwargs):
        if wakeup:
            self.wakeup_workers(channel=channel)

    def itercapture(self, limit=None, timeout=None, wakeup=True):
        return self.consume(limit=limit, timeout=timeout, wakeup=wakeup)

    def capture(self, limit=None, timeout=None, wakeup=True):
        """Open up a consumer capturing events.

        This has to run in the main process, and it will never stop
        unless :attr:`EventDispatcher.should_stop` is set to True, or
        forced via :exc:`KeyboardInterrupt` or :exc:`SystemExit`.
        """
        return list(self.consume(limit=limit, timeout=timeout, wakeup=wakeup))

    def wakeup_workers(self, channel=None):
        self.app.control.broadcast('heartbeat',
                                   connection=self.connection,
                                   channel=channel)

    def event_from_message(self, body, localize=True,
                           now=time.time, tzfields=_TZGETTER,
                           adjust_timestamp=adjust_timestamp,
                           CLIENT_CLOCK_SKEW=CLIENT_CLOCK_SKEW):
        type = body['type']
        if type == 'task-sent':
            # clients never sync so cannot use their clock value
            _c = body['clock'] = (self.clock.value or 1) + CLIENT_CLOCK_SKEW
            self.adjust_clock(_c)
        else:
            try:
                clock = body['clock']
            except KeyError:
                body['clock'] = self.forward_clock()
            else:
                self.adjust_clock(clock)

        if localize:
            try:
                offset, timestamp = tzfields(body)
            except KeyError:
                pass
            else:
                body['timestamp'] = adjust_timestamp(timestamp, offset)
        body['local_received'] = now()
        return type, body

    def _receive(self, body, message, list=list, isinstance=isinstance):
        if isinstance(body, list):  # celery 4.0: List of events
            process, from_message = self.process, self.event_from_message
            [process(*from_message(event)) for event in body]
        else:
            self.process(*self.event_from_message(body))

    @property
    def connection(self):
        return self.channel.connection.client if self.channel else None


class Events(object):
    """Implements app.events."""

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
        with self.app.amqp.producer_pool.acquire(block=True) as prod:
            # pylint: disable=too-many-function-args
            # This is a property pylint...
            with self.Dispatcher(prod.connection, hostname, enabled,
                                 prod.channel, buffer_while_offline) as d:
                yield d
