# -*- coding: utf-8 -*-
"""
    celery.app.amqp
    ~~~~~~~~~~~~~~~

    Sending and receiving messages using Kombu.

"""
from __future__ import absolute_import

import numbers

from collections import Mapping, namedtuple
from datetime import timedelta
from weakref import WeakValueDictionary

from kombu import Connection, Consumer, Exchange, Producer, Queue
from kombu.common import Broadcast
from kombu.pools import ProducerPool
from kombu.utils import cached_property
from kombu.utils.encoding import safe_repr
from kombu.utils.functional import maybe_list

from celery import signals
from celery.five import items, string_t
from celery.utils.text import indent as textindent
from celery.utils.timeutils import to_utc

from . import routes as _routes

__all__ = ['AMQP', 'Queues', 'task_message']

#: Human readable queue declaration.
QUEUE_FORMAT = """
.> {0.name:<16} exchange={0.exchange.name}({0.exchange.type}) \
key={0.routing_key}
"""

task_message = namedtuple('task_message',
                          ('headers', 'properties', 'body', 'sent_event'))


class Queues(dict):
    """Queue name⇒ declaration mapping.

    :param queues: Initial list/tuple or dict of queues.
    :keyword create_missing: By default any unknown queues will be
                             added automatically, but if disabled
                             the occurrence of unknown queues
                             in `wanted` will raise :exc:`KeyError`.
    :keyword ha_policy: Default HA policy for queues with none set.


    """
    #: If set, this is a subset of queues to consume from.
    #: The rest of the queues are then used for routing only.
    _consume_from = None

    def __init__(self, queues=None, default_exchange=None,
                 create_missing=True, ha_policy=None, autoexchange=None):
        dict.__init__(self)
        self.aliases = WeakValueDictionary()
        self.default_exchange = default_exchange
        self.create_missing = create_missing
        self.ha_policy = ha_policy
        self.autoexchange = Exchange if autoexchange is None else autoexchange
        if isinstance(queues, (tuple, list)):
            queues = {q.name: q for q in queues}
        for name, q in items(queues or {}):
            self.add(q) if isinstance(q, Queue) else self.add_compat(name, **q)

    def __getitem__(self, name):
        try:
            return self.aliases[name]
        except KeyError:
            return dict.__getitem__(self, name)

    def __setitem__(self, name, queue):
        if self.default_exchange and (not queue.exchange or
                                      not queue.exchange.name):
            queue.exchange = self.default_exchange
        dict.__setitem__(self, name, queue)
        if queue.alias:
            self.aliases[queue.alias] = queue

    def __missing__(self, name):
        if self.create_missing:
            return self.add(self.new_missing(name))
        raise KeyError(name)

    def add(self, queue, **kwargs):
        """Add new queue.

        The first argument can either be a :class:`kombu.Queue` instance,
        or the name of a queue.  If the former the rest of the keyword
        arguments are ignored, and options are simply taken from the queue
        instance.

        :param queue: :class:`kombu.Queue` instance or name of the queue.
        :keyword exchange: (if named) specifies exchange name.
        :keyword routing_key: (if named) specifies binding key.
        :keyword exchange_type: (if named) specifies type of exchange.
        :keyword \*\*options: (if named) Additional declaration options.

        """
        if not isinstance(queue, Queue):
            return self.add_compat(queue, **kwargs)
        if self.ha_policy:
            if queue.queue_arguments is None:
                queue.queue_arguments = {}
            self._set_ha_policy(queue.queue_arguments)
        self[queue.name] = queue
        return queue

    def add_compat(self, name, **options):
        # docs used to use binding_key as routing key
        options.setdefault('routing_key', options.get('binding_key'))
        if options['routing_key'] is None:
            options['routing_key'] = name
        if self.ha_policy is not None:
            self._set_ha_policy(options.setdefault('queue_arguments', {}))
        q = self[name] = Queue.from_dict(name, **options)
        return q

    def _set_ha_policy(self, args):
        policy = self.ha_policy
        if isinstance(policy, (list, tuple)):
            return args.update({'x-ha-policy': 'nodes',
                                'x-ha-policy-params': list(policy)})
        args['x-ha-policy'] = policy

    def format(self, indent=0, indent_first=True):
        """Format routing table into string for log dumps."""
        active = self.consume_from
        if not active:
            return ''
        info = [QUEUE_FORMAT.strip().format(q)
                for _, q in sorted(items(active))]
        if indent_first:
            return textindent('\n'.join(info), indent)
        return info[0] + '\n' + textindent('\n'.join(info[1:]), indent)

    def select_add(self, queue, **kwargs):
        """Add new task queue that will be consumed from even when
        a subset has been selected using the :option:`-Q` option."""
        q = self.add(queue, **kwargs)
        if self._consume_from is not None:
            self._consume_from[q.name] = q
        return q

    def select(self, include):
        """Sets :attr:`consume_from` by selecting a subset of the
        currently defined queues.

        :param include: Names of queues to consume from.
                        Can be iterable or string.
        """
        if include:
            self._consume_from = {
                name: self[name] for name in maybe_list(include)
            }
    select_subset = select  # XXX compat

    def deselect(self, exclude):
        """Deselect queues so that they will not be consumed from.

        :param exclude: Names of queues to avoid consuming from.
                        Can be iterable or string.

        """
        if exclude:
            exclude = maybe_list(exclude)
            if self._consume_from is None:
                # using selection
                return self.select(k for k in self if k not in exclude)
            # using all queues
            for queue in exclude:
                self._consume_from.pop(queue, None)
    select_remove = deselect  # XXX compat

    def new_missing(self, name):
        return Queue(name, self.autoexchange(name), name)

    @property
    def consume_from(self):
        if self._consume_from is not None:
            return self._consume_from
        return self


class AMQP(object):
    Connection = Connection
    Consumer = Consumer
    Producer = Producer

    #: compat alias to Connection
    BrokerConnection = Connection

    queues_cls = Queues

    #: Cached and prepared routing table.
    _rtable = None

    #: Underlying producer pool instance automatically
    #: set by the :attr:`producer_pool`.
    _producer_pool = None

    # Exchange class/function used when defining automatic queues.
    # E.g. you can use ``autoexchange = lambda n: None`` to use the
    # amqp default exchange, which is a shortcut to bypass routing
    # and instead send directly to the queue named in the routing key.
    autoexchange = None

    def __init__(self, app):
        self.app = app
        self.task_protocols = {
            1: self.as_task_v1,
            2: self.as_task_v2,
        }

    @cached_property
    def create_task_message(self):
        return self.task_protocols[self.app.conf.CELERY_TASK_PROTOCOL]

    @cached_property
    def send_task_message(self):
        return self._create_task_sender()

    def Queues(self, queues, create_missing=None, ha_policy=None,
               autoexchange=None):
        """Create new :class:`Queues` instance, using queue defaults
        from the current configuration."""
        conf = self.app.conf
        if create_missing is None:
            create_missing = conf.CELERY_CREATE_MISSING_QUEUES
        if ha_policy is None:
            ha_policy = conf.CELERY_QUEUE_HA_POLICY
        if not queues and conf.CELERY_DEFAULT_QUEUE:
            queues = (Queue(conf.CELERY_DEFAULT_QUEUE,
                            exchange=self.default_exchange,
                            routing_key=conf.CELERY_DEFAULT_ROUTING_KEY), )
        autoexchange = (self.autoexchange if autoexchange is None
                        else autoexchange)
        return self.queues_cls(
            queues, self.default_exchange, create_missing,
            ha_policy, autoexchange,
        )

    def Router(self, queues=None, create_missing=None):
        """Return the current task router."""
        return _routes.Router(self.routes, queues or self.queues,
                              self.app.either('CELERY_CREATE_MISSING_QUEUES',
                                              create_missing), app=self.app)

    def flush_routes(self):
        self._rtable = _routes.prepare(self.app.conf.CELERY_ROUTES)

    def TaskConsumer(self, channel, queues=None, accept=None, **kw):
        if accept is None:
            accept = self.app.conf.CELERY_ACCEPT_CONTENT
        return self.Consumer(
            channel, accept=accept,
            queues=queues or list(self.queues.consume_from.values()),
            **kw
        )

    def as_task_v2(self, task_id, name, args=None, kwargs=None,
                   countdown=None, eta=None, group_id=None,
                   expires=None, retries=0, chord=None,
                   callbacks=None, errbacks=None, reply_to=None,
                   time_limit=None, soft_time_limit=None,
                   create_sent_event=False, root_id=None, parent_id=None,
                   now=None, timezone=None):
        args = args or ()
        kwargs = kwargs or {}
        utc = self.utc
        if not isinstance(args, (list, tuple)):
            raise TypeError('task args must be a list or tuple')
        if not isinstance(kwargs, Mapping):
            raise TypeError('task keyword arguments must be a mapping')
        if countdown:  # convert countdown to ETA
            now = now or self.app.now()
            timezone = timezone or self.app.timezone
            eta = now + timedelta(seconds=countdown)
            if utc:
                eta = to_utc(eta).astimezone(timezone)
        if isinstance(expires, numbers.Real):
            now = now or self.app.now()
            timezone = timezone or self.app.timezone
            expires = now + timedelta(seconds=expires)
            if utc:
                expires = to_utc(expires).astimezone(timezone)
        eta = eta and eta.isoformat()
        expires = expires and expires.isoformat()

        return task_message(
            headers={
                'lang': 'py',
                'task': name,
                'id': task_id,
                'eta': eta,
                'expires': expires,
                'group': group_id,
                'retries': retries,
                'timelimit': [time_limit, soft_time_limit],
                'root_id': root_id,
                'parent_id': parent_id,
            },
            properties={
                'correlation_id': task_id,
                'reply_to': reply_to or '',
            },
            body=(
                args, kwargs, {
                    'callbacks': callbacks,
                    'errbacks': errbacks,
                    'chain': None,  # TODO
                    'chord': chord,
                },
            ),
            sent_event={
                'uuid': task_id,
                'root': root_id,
                'parent': parent_id,
                'name': name,
                'args': safe_repr(args),
                'kwargs': safe_repr(kwargs),
                'retries': retries,
                'eta': eta,
                'expires': expires,
            } if create_sent_event else None,
        )

    def as_task_v1(self, task_id, name, args=None, kwargs=None,
                   countdown=None, eta=None, group_id=None,
                   expires=None, retries=0,
                   chord=None, callbacks=None, errbacks=None, reply_to=None,
                   time_limit=None, soft_time_limit=None,
                   create_sent_event=False, root_id=None, parent_id=None,
                   now=None, timezone=None):
        args = args or ()
        kwargs = kwargs or {}
        utc = self.utc
        if not isinstance(args, (list, tuple)):
            raise ValueError('task args must be a list or tuple')
        if not isinstance(kwargs, Mapping):
            raise ValueError('task keyword arguments must be a mapping')
        if countdown:  # convert countdown to ETA
            now = now or self.app.now()
            timezone = timezone or self.app.timezone
            eta = now + timedelta(seconds=countdown)
            if utc:
                eta = to_utc(eta).astimezone(timezone)
        if isinstance(expires, numbers.Real):
            now = now or self.app.now()
            timezone = timezone or self.app.timezone
            expires = now + timedelta(seconds=expires)
            if utc:
                expires = to_utc(expires).astimezone(timezone)
        eta = eta and eta.isoformat()
        expires = expires and expires.isoformat()

        return task_message(
            headers={},
            properties={
                'correlation_id': task_id,
                'reply_to': reply_to or '',
            },
            body={
                'task': name,
                'id': task_id,
                'args': args,
                'kwargs': kwargs,
                'retries': retries,
                'eta': eta,
                'expires': expires,
                'utc': utc,
                'callbacks': callbacks,
                'errbacks': errbacks,
                'timelimit': (time_limit, soft_time_limit),
                'taskset': group_id,
                'chord': chord,
            },
            sent_event={
                'uuid': task_id,
                'name': name,
                'args': safe_repr(args),
                'kwargs': safe_repr(kwargs),
                'retries': retries,
                'eta': eta,
                'expires': expires,
            } if create_sent_event else None,
        )

    def _create_task_sender(self):
        default_retry = self.app.conf.CELERY_TASK_PUBLISH_RETRY
        default_policy = self.app.conf.CELERY_TASK_PUBLISH_RETRY_POLICY
        default_delivery_mode = self.app.conf.CELERY_DEFAULT_DELIVERY_MODE
        default_queue = self.default_queue
        queues = self.queues
        send_before_publish = signals.before_task_publish.send
        before_receivers = signals.before_task_publish.receivers
        send_after_publish = signals.after_task_publish.send
        after_receivers = signals.after_task_publish.receivers

        send_task_sent = signals.task_sent.send   # XXX compat
        sent_receivers = signals.task_sent.receivers

        default_evd = self._event_dispatcher
        default_exchange = self.default_exchange

        default_rkey = self.app.conf.CELERY_DEFAULT_ROUTING_KEY
        default_serializer = self.app.conf.CELERY_TASK_SERIALIZER
        default_compressor = self.app.conf.CELERY_MESSAGE_COMPRESSION

        def publish_task(producer, name, message,
                         exchange=None, routing_key=None, queue=None,
                         event_dispatcher=None, retry=None, retry_policy=None,
                         serializer=None, delivery_mode=None,
                         compression=None, declare=None,
                         headers=None, **kwargs):
            retry = default_retry if retry is None else retry
            headers2, properties, body, sent_event = message
            if headers:
                headers2.update(headers)
            if kwargs:
                properties.update(kwargs)

            qname = queue
            if queue is None and exchange is None:
                queue = default_queue
            if queue is not None:
                if isinstance(queue, string_t):
                    qname, queue = queue, queues[queue]
                else:
                    qname = queue.name
            if delivery_mode is None:
                try:
                    delivery_mode = queue.exchange.delivery_mode
                except AttributeError:
                    delivery_mode = default_delivery_mode
            exchange = exchange or queue.exchange.name
            routing_key = routing_key or queue.routing_key
            if declare is None and queue and not isinstance(queue, Broadcast):
                declare = [queue]

            # merge default and custom policy
            retry = default_retry if retry is None else retry
            _rp = (dict(default_policy, **retry_policy) if retry_policy
                   else default_policy)

            if before_receivers:
                send_before_publish(
                    sender=name, body=body,
                    exchange=exchange, routing_key=routing_key,
                    declare=declare, headers=headers2,
                    properties=kwargs,  retry_policy=retry_policy,
                )
            ret = producer.publish(
                body,
                exchange=exchange or default_exchange,
                routing_key=routing_key or default_rkey,
                serializer=serializer or default_serializer,
                compression=compression or default_compressor,
                retry=retry, retry_policy=_rp,
                delivery_mode=delivery_mode, declare=declare,
                headers=headers2,
                **properties
            )
            if after_receivers:
                send_after_publish(sender=name, body=body, headers=headers2,
                                   exchange=exchange, routing_key=routing_key)
            if sent_receivers:  # XXX deprecated
                send_task_sent(sender=name, task_id=body['id'], task=name,
                               args=body['args'], kwargs=body['kwargs'],
                               eta=body['eta'], taskset=body['taskset'])
            if sent_event:
                evd = event_dispatcher or default_evd
                exname = exchange or self.exchange
                if isinstance(name, Exchange):
                    exname = exname.name
                sent_event.update({
                    'queue': qname,
                    'exchange': exname,
                    'routing_key': routing_key,
                })
                evd.publish('task-sent', sent_event,
                            self, retry=retry, retry_policy=retry_policy)
            return ret
        return publish_task

    @cached_property
    def default_queue(self):
        return self.queues[self.app.conf.CELERY_DEFAULT_QUEUE]

    @cached_property
    def queues(self):
        """Queue name⇒ declaration mapping."""
        return self.Queues(self.app.conf.CELERY_QUEUES)

    @queues.setter  # noqa
    def queues(self, queues):
        return self.Queues(queues)

    @property
    def routes(self):
        if self._rtable is None:
            self.flush_routes()
        return self._rtable

    @cached_property
    def router(self):
        return self.Router()

    @property
    def producer_pool(self):
        if self._producer_pool is None:
            self._producer_pool = ProducerPool(
                self.app.pool,
                limit=self.app.pool.limit,
                Producer=self.Producer,
            )
        return self._producer_pool
    publisher_pool = producer_pool  # compat alias

    @cached_property
    def default_exchange(self):
        return Exchange(self.app.conf.CELERY_DEFAULT_EXCHANGE,
                        self.app.conf.CELERY_DEFAULT_EXCHANGE_TYPE)

    @cached_property
    def utc(self):
        return self.app.conf.CELERY_ENABLE_UTC

    @cached_property
    def _event_dispatcher(self):
        # We call Dispatcher.publish with a custom producer
        # so don't need the diuspatcher to be enabled.
        return self.app.events.Dispatcher(enabled=False)
