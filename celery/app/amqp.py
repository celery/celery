# -*- coding: utf-8 -*-
"""Sending/Receiving Messages (Kombu integration)."""
import numbers

from collections import Mapping
from datetime import datetime, timedelta, tzinfo
from typing import (
    Any, Callable, MutableMapping, NamedTuple, Set, Sequence, Union,
    cast,
)
from weakref import WeakValueDictionary

from kombu import pools
from kombu import Connection, Consumer, Exchange, Producer, Queue
from kombu.common import Broadcast
from kombu.types import ChannelT, ConsumerT, EntityT, ProducerT, ResourceT
from kombu.utils.functional import maybe_list
from kombu.utils.objects import cached_property

from celery import signals
from celery.events import EventDispatcher
from celery.types import AppT, ResultT, RouterT, SignatureT
from celery.utils.nodenames import anon_nodename
from celery.utils.saferepr import saferepr
from celery.utils.text import indent as textindent
from celery.utils.time import maybe_make_aware, to_utc

from . import routes as _routes

__all__ = ['AMQP', 'Queues', 'task_message']

#: earliest date supported by time.mktime.
INT_MIN = -2147483648

#: Human readable queue declaration.
QUEUE_FORMAT = """
.> {0.name:<16} exchange={0.exchange.name}({0.exchange.type}) \
key={0.routing_key}
"""

QueuesArgT = Union[Mapping[str, Queue], Sequence[Queue]]


class task_message(NamedTuple):
    """Represents a task message that can be sent."""

    headers: MutableMapping
    properties: MutableMapping
    body: Any
    sent_event: Mapping


def utf8dict(d: Mapping, encoding: str = 'utf-8') -> Mapping:
    return {k.decode(encoding) if isinstance(k, bytes) else k: v
            for k, v in d.items()}


class Queues(dict):
    """Queue name⇒ declaration mapping.

    Arguments:
        queues (Iterable): Initial list/tuple or dict of queues.
        create_missing (bool): By default any unknown queues will be
            added automatically, but if this flag is disabled the occurrence
            of unknown queues in `wanted` will raise :exc:`KeyError`.
        ha_policy (Sequence, str): Default HA policy for queues with none set.
        max_priority (int): Default x-max-priority for queues with none set.
    """

    #: If set, this is a subset of queues to consume from.
    #: The rest of the queues are then used for routing only.
    _consume_from: Mapping[str, Queue] = None

    def __init__(self,
                 queues: QueuesArgT = None,
                 default_exchange: str = None,
                 create_missing: bool = True,
                 ha_policy: Union[Sequence, str] = None,
                 autoexchange: Callable[[str], Exchange] = None,
                 max_priority: int = None,
                 default_routing_key: str = None) -> None:
        dict.__init__(self)
        self.aliases = WeakValueDictionary()
        self.default_exchange = default_exchange
        self.default_routing_key = default_routing_key
        self.create_missing = create_missing
        self.ha_policy = ha_policy
        self.autoexchange = Exchange if autoexchange is None else autoexchange
        self.max_priority = max_priority
        if queues is not None and not isinstance(queues, Mapping):
            queues = {q.name: q for q in queues}
        for name, q in (queues or {}).items():
            self.add(q) if isinstance(q, Queue) else self.add_compat(name, **q)

    def __getitem__(self, name: str) -> Queue:
        try:
            return self.aliases[name]
        except KeyError:
            return dict.__getitem__(self, name)

    def __setitem__(self, name: str, queue: Queue) -> None:
        if self.default_exchange and not queue.exchange:
            queue.exchange = self.default_exchange
        dict.__setitem__(self, name, queue)
        if queue.alias:
            self.aliases[queue.alias] = queue

    def __missing__(self, name: str) -> Queue:
        if self.create_missing:
            return self.add(self.new_missing(name))
        raise KeyError(name)

    def add(self, queue: Union[Queue, str], **kwargs) -> Queue:
        """Add new queue.

        The first argument can either be a :class:`kombu.Queue` instance,
        or the name of a queue.  If the former the rest of the keyword
        arguments are ignored, and options are simply taken from the queue
        instance.

        Arguments:
            queue (kombu.Queue, str): Queue to add.
            exchange (kombu.Exchange, str):
                if queue is str, specifies exchange name.
            routing_key (str): if queue is str, specifies binding key.
            exchange_type (str): if queue is str, specifies type of exchange.
            **options (Any): Additional declaration options used when
                queue is a str.
        """
        if not isinstance(queue, Queue):
            return self.add_compat(queue, **kwargs)
        return self._add(queue)

    def add_compat(self, name: str, **options) -> Queue:
        # docs used to use binding_key as routing key
        options.setdefault('routing_key', options.get('binding_key'))
        if options['routing_key'] is None:
            options['routing_key'] = name
        return self._add(Queue.from_dict(name, **options))

    def _add(self, queue: Queue) -> Queue:
        if not queue.routing_key:
            if queue.exchange is None or queue.exchange.name == '':
                queue.exchange = self.default_exchange
            queue.routing_key = self.default_routing_key
        if self.ha_policy:
            if queue.queue_arguments is None:
                queue.queue_arguments = {}
            self._set_ha_policy(queue.queue_arguments)
        if self.max_priority is not None:
            if queue.queue_arguments is None:
                queue.queue_arguments = {}
            self._set_max_priority(queue.queue_arguments)
        self[queue.name] = queue
        return queue

    def _set_ha_policy(self, args: MutableMapping) -> None:
        policy = self.ha_policy
        if isinstance(policy, (list, tuple)):
            args.update({'x-ha-policy': 'nodes',
                         'x-ha-policy-params': list(policy)})
        else:
            args['x-ha-policy'] = policy

    def _set_max_priority(self, args: MutableMapping) -> None:
        if 'x-max-priority' not in args and self.max_priority is not None:
            args.update({'x-max-priority': self.max_priority})

    def format(self, indent: int = 0, indent_first: bool = True) -> str:
        """Format routing table into string for log dumps."""
        active = self.consume_from
        if not active:
            return ''
        info = [QUEUE_FORMAT.strip().format(q)
                for _, q in sorted(active.items())]
        if indent_first:
            return textindent('\n'.join(info), indent)
        return info[0] + '\n' + textindent('\n'.join(info[1:]), indent)

    def select_add(self, queue: Queue, **kwargs) -> Queue:
        """Add new task queue that'll be consumed from.

        The queue will be active even when a subset has been selected
        using the :option:`celery worker -Q` option.
        """
        q = self.add(queue, **kwargs)
        if self._consume_from is not None:
            self._consume_from[q.name] = q
        return q

    def select(self, include: Union[Sequence[str], str]) -> None:
        """Select a subset of currently defined queues to consume from.

        Arguments:
            include (Sequence[str], str): Names of queues to consume from.
        """
        if include:
            self._consume_from = {
                name: self[name] for name in maybe_list(include)
            }

    def deselect(self, exclude: Union[Sequence[str], str]) -> None:
        """Deselect queues so that they won't be consumed from.

        Arguments:
            exclude (Sequence[str], str): Names of queues to avoid
                consuming from.
        """
        if exclude:
            exclude = maybe_list(exclude)
            if self._consume_from is None:
                # using selection
                self.select(k for k in self if k not in exclude)
            else:
                # using all queues
                for queue in exclude:
                    self._consume_from.pop(queue, None)

    def new_missing(self, name: str) -> Queue:
        return Queue(name, self.autoexchange(name), name)

    @property
    def consume_from(self) -> Mapping[str, Queue]:
        if self._consume_from is not None:
            return self._consume_from
        return cast(Mapping[str, Queue], self)


class AMQP:
    """App AMQP API: app.amqp."""

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
    _producer_pool: ResourceT = None

    # Exchange class/function used when defining automatic queues.
    # For example, you can use ``autoexchange = lambda n: None`` to use the
    # AMQP default exchange: a shortcut to bypass routing
    # and instead send directly to the queue named in the routing key.
    autoexchange: Callable[[str], Exchange] = None

    #: Max size of positional argument representation used for
    #: logging purposes.
    argsrepr_maxsize = 1024

    #: Max size of keyword argument representation used for logging purposes.
    kwargsrepr_maxsize = 1024

    task_protocols: Mapping[int, Callable] = None

    def __init__(self, app: AppT) -> None:
        self.app = app
        self.task_protocols = {
            1: self.as_task_v1,
            2: self.as_task_v2,
        }

    @cached_property
    def create_task_message(self) -> Callable:
        return self.task_protocols[self.app.conf.task_protocol]

    @cached_property
    def send_task_message(self) -> Callable:
        return self._create_task_sender()

    def Queues(self, queues: QueuesArgT,
               create_missing: bool = None,
               ha_policy: Union[Sequence, str] = None,
               autoexchange: Callable[[str], Exchange] = None,
               max_priority: int = None) -> Queues:
        # Create new :class:`Queues` instance, using queue defaults
        # from the current configuration.
        conf = self.app.conf
        default_routing_key = conf.task_default_routing_key
        if create_missing is None:
            create_missing = conf.task_create_missing_queues
        if ha_policy is None:
            ha_policy = conf.task_queue_ha_policy
        if max_priority is None:
            max_priority = conf.task_queue_max_priority
        if not queues and conf.task_default_queue:
            queues = (Queue(conf.task_default_queue,
                            exchange=self.default_exchange,
                            routing_key=default_routing_key),)
        autoexchange = (self.autoexchange
                        if autoexchange is None else autoexchange)
        return self.queues_cls(
            queues, self.default_exchange, create_missing,
            ha_policy, autoexchange, max_priority, default_routing_key,
        )

    def Router(self, queues: Mapping[str, Queue] = None,
               create_missing: bool = None) -> RouterT:
        """Return the current task router."""
        return _routes.Router(self.routes, queues or self.queues,
                              self.app.either('task_create_missing_queues',
                                              create_missing), app=self.app)

    def flush_routes(self) -> None:
        self._rtable = _routes.prepare(self.app.conf.task_routes)

    def TaskConsumer(self, channel: ChannelT,
                     queues: Mapping[str, Queue] = None,
                     accept: Set[str] = None,
                     **kw) -> ConsumerT:
        if accept is None:
            accept = self.app.conf.accept_content
        return self.Consumer(
            channel, accept=accept,
            queues=queues or list(self.queues.consume_from.values()),
            **kw
        )

    def as_task_v2(self, task_id: str, name: str, *,
                   args: Sequence = None,
                   kwargs: Mapping = None,
                   countdown: float = None,
                   eta: datetime = None,
                   group_id: str = None,
                   expires: Union[float, datetime] = None,
                   retries: int = 0,
                   chord: SignatureT = None,
                   callbacks: Sequence[SignatureT] = None,
                   errbacks: Sequence[SignatureT] = None,
                   reply_to: str = None,
                   time_limit: float = None,
                   soft_time_limit: float = None,
                   create_sent_event: bool = False,
                   root_id: str = None,
                   parent_id: str = None,
                   shadow: str = None,
                   chain: Sequence[SignatureT] = None,
                   now: datetime = None,
                   timezone: tzinfo = None,
                   origin: str = None,
                   argsrepr: str = None,
                   kwargsrepr: str = None) -> task_message:
        args = args or ()
        kwargs = kwargs or {}
        if not isinstance(args, (list, tuple)):
            raise TypeError('task args must be a list or tuple')
        if not isinstance(kwargs, Mapping):
            raise TypeError('task keyword arguments must be a mapping')
        if countdown:  # convert countdown to ETA
            self._verify_seconds(countdown, 'countdown')
            now = now or self.app.now()
            timezone = timezone or self.app.timezone
            eta = maybe_make_aware(
                now + timedelta(seconds=countdown), tz=timezone,
            )
        if isinstance(expires, numbers.Real):
            self._verify_seconds(expires, 'expires')
            now = now or self.app.now()
            timezone = timezone or self.app.timezone
            expires = maybe_make_aware(
                now + timedelta(seconds=expires), tz=timezone,
            )
        eta = eta and eta.isoformat()
        expires = expires and expires.isoformat()

        if argsrepr is None:
            argsrepr = saferepr(args, self.argsrepr_maxsize)
        if kwargsrepr is None:
            kwargsrepr = saferepr(kwargs, self.kwargsrepr_maxsize)

        if not root_id:  # empty root_id defaults to task_id
            root_id = task_id

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
                'argsrepr': argsrepr,
                'kwargsrepr': kwargsrepr,
                'origin': origin or anon_nodename()
            },
            properties={
                'correlation_id': task_id,
                'reply_to': reply_to or '',
            },
            body=(
                args, kwargs, {
                    'callbacks': callbacks,
                    'errbacks': errbacks,
                    'chain': chain,
                    'chord': chord,
                },
            ),
            sent_event={
                'uuid': task_id,
                'root_id': root_id,
                'parent_id': parent_id,
                'name': name,
                'args': argsrepr,
                'kwargs': kwargsrepr,
                'retries': retries,
                'eta': eta,
                'expires': expires,
            } if create_sent_event else None,
        )

    def as_task_v1(self, task_id, name,
                   args: Sequence = None,
                   kwargs: Mapping = None,
                   countdown: float = None,
                   eta: datetime = None,
                   group_id: str = None,
                   expires: Union[float, datetime] = None,
                   retries: int = 0,
                   chord: SignatureT = None,
                   callbacks: Sequence[SignatureT] = None,
                   errbacks: Sequence[SignatureT] = None,
                   reply_to: str = None,
                   time_limit: float = None,
                   soft_time_limit: float = None,
                   create_sent_event: bool = False,
                   root_id: str = None,
                   parent_id: str = None,
                   shadow: str = None,
                   now: datetime = None,
                   timezone: tzinfo = None) -> task_message:
        args = args or ()
        kwargs = kwargs or {}
        utc = self.utc
        if not isinstance(args, (list, tuple)):
            raise TypeError('task args must be a list or tuple')
        if not isinstance(kwargs, Mapping):
            raise TypeError('task keyword arguments must be a mapping')
        if countdown:  # convert countdown to ETA
            self._verify_seconds(countdown, 'countdown')
            now = now or self.app.now()
            timezone = timezone or self.app.timezone
            eta = now + timedelta(seconds=countdown)
            if utc:
                eta = to_utc(eta).astimezone(timezone)
        if isinstance(expires, numbers.Real):
            self._verify_seconds(expires, 'expires')
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
                'group': group_id,
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
                'args': saferepr(args),
                'kwargs': saferepr(kwargs),
                'retries': retries,
                'eta': eta,
                'expires': expires,
            } if create_sent_event else None,
        )

    def _verify_seconds(self, s: float, what: str) -> float:
        if s < INT_MIN:
            raise ValueError('%s is out of range: %r' % (what, s))
        return s

    def _create_task_sender(self) -> Callable:
        default_retry = self.app.conf.task_publish_retry
        default_policy = self.app.conf.task_publish_retry_policy
        default_delivery_mode = self.app.conf.task_default_delivery_mode
        default_queue = self.default_queue
        queues = self.queues
        send_before_publish = signals.before_task_publish.send
        before_receivers = signals.before_task_publish.receivers
        send_after_publish = signals.after_task_publish.send
        after_receivers = signals.after_task_publish.receivers

        default_evd = self._event_dispatcher
        default_exchange = self.default_exchange

        default_rkey = self.app.conf.task_default_routing_key
        default_serializer = self.app.conf.task_serializer
        default_compressor = self.app.conf.result_compression

        def send_task_message(
                producer: ProducerT, name: str, message: task_message,
                *,
                exchange: Union[Exchange, str] = None,
                routing_key: str = None,
                queue: str = None,
                event_dispatcher: EventDispatcher = None,
                retry: bool = None,
                retry_policy: Mapping[str, Any] = None,
                serializer: str = None,
                delivery_mode: Union[str, int] = None,
                compression: str = None,
                declare: Sequence[EntityT] = None,
                headers: Mapping = None,
                exchange_type: str = None,
                **kwargs) -> ResultT:
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
                if isinstance(queue, str):
                    qname, queue = queue, queues[queue]
                else:
                    qname = queue.name

            if delivery_mode is None:
                try:
                    delivery_mode = queue.exchange.delivery_mode
                except AttributeError:
                    pass
                delivery_mode = delivery_mode or default_delivery_mode

            if exchange_type is None:
                try:
                    exchange_type = queue.exchange.type
                except AttributeError:
                    exchange_type = 'direct'

            # convert to anon-exchange, when exchange not set and direct ex.
            if not exchange or not routing_key and exchange_type == 'direct':
                    exchange, routing_key = '', qname
            elif exchange is None:
                # not topic exchange, and exchange not undefined
                exchange = queue.exchange.name or default_exchange
                routing_key = routing_key or queue.routing_key or default_rkey
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
                    properties=kwargs, retry_policy=retry_policy,
                )
            ret = producer.publish(
                body,
                exchange=exchange,
                routing_key=routing_key,
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
            if sent_event:
                evd = event_dispatcher or default_evd
                exname = exchange
                if isinstance(exname, Exchange):
                    exname = exname.name
                sent_event.update({
                    'queue': qname,
                    'exchange': exname,
                    'routing_key': routing_key,
                })
                evd.publish('task-sent', sent_event,
                            producer, retry=retry, retry_policy=retry_policy)
            return ret
        return send_task_message

    @cached_property
    def default_queue(self) -> Queue:
        return self.queues[self.app.conf.task_default_queue]

    @cached_property
    def queues(self) -> Queues:
        """Queue name⇒ declaration mapping."""
        return self.Queues(self.app.conf.task_queues)

    @queues.setter  # noqa
    def queues(self, queues: QueuesArgT) -> Queues:
        return self.Queues(queues)

    @property
    def routes(self) -> Sequence[RouterT]:
        if self._rtable is None:
            self.flush_routes()
        return self._rtable

    @cached_property
    def router(self) -> RouterT:
        return self.Router()

    @property
    def producer_pool(self) -> ResourceT:
        if self._producer_pool is None:
            self._producer_pool = pools.producers[
                self.app.connection_for_write()]
            self._producer_pool.limit = self.app.pool.limit
        return self._producer_pool

    @cached_property
    def default_exchange(self) -> Exchange:
        return Exchange(self.app.conf.task_default_exchange,
                        self.app.conf.task_default_exchange_type)

    @cached_property
    def utc(self) -> bool:
        return self.app.conf.enable_utc

    @cached_property
    def _event_dispatcher(self) -> EventDispatcher:
        # We call Dispatcher.publish with a custom producer
        # so don't need the diuspatcher to be enabled.
        return self.app.events.Dispatcher(enabled=False)
