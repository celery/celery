# -*- coding: utf-8 -*-
"""
    celery.app.amqp
    ~~~~~~~~~~~~~~~

    AMQ related functionality.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from datetime import timedelta
from weakref import WeakValueDictionary

from kombu import BrokerConnection, Consumer, Exchange, Producer, Queue
from kombu.common import entry_to_queue, maybe_declare
from kombu.pools import ProducerPool

from celery import signals
from celery.utils import cached_property, lpmerge, uuid
from celery.utils import text

from . import routes as _routes

#: Human readable queue declaration.
QUEUE_FORMAT = """
. %(name)s exchange:%(exchange)s(%(exchange_type)s) binding:%(routing_key)s
"""


class Queues(dict):
    """Queue name⇒ declaration mapping.

    :param queues: Initial list/tuple or dict of queues.

    """
    #: If set, this is a subset of queues to consume from.
    #: The rest of the queues are then used for routing only.
    _consume_from = None

    def __init__(self, queues):
        self.aliases = WeakValueDictionary()
        if isinstance(queues, (tuple, list)):
            queues = dict((q.name, q) for q in queues)
        dict.__init__(self)
        for name, q in (queues or {}).items():
            self.add(q) if isinstance(q, Queue) else self.add_compat(name, **q)

    def __getitem__(self, name):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return self.aliases[key]

    def add(self, queue, **kwargs):
        """Add new queue.

        :param queue: Name of the queue.
        :keyword exchange: Name of the exchange.
        :keyword routing_key: Binding key.
        :keyword exchange_type: Type of exchange.
        :keyword \*\*options: Additional declaration options.

        """
        if not isinstance(queue, Queue):
            return self.add_compat(queue, **kwargs)
        self[queue.name] = queue
        if queue.alias:
            self.aliases[queue.alias] = queue
        return queue

    def add_compat(self, name, **options):
        # docs used to use binding_key as routing key
        options.setdefault("routing_key", options.get("binding_key"))
        self[name] = queue = entry_to_queue(name, **options)
        return queue

    def options(self, exchange, routing_key,
            exchange_type="direct", **options):
        """Creates new option mapping for queue, with required
        keys present."""
        return dict(options, routing_key=routing_key,
                             exchange=exchange,
                             exchange_type=exchange_type)

    def format(self, indent=0, indent_first=True):
        """Format routing table into string for log dumps."""
        active = self.consume_from
        if not active:
            return ""
        info = [QUEUE_FORMAT.strip() % {
                    "name": (name + ":").ljust(12),
                    "exchange": q.exchange.name,
                    "exchange_type": q.exchange.type,
                    "routing_key": q.routing_key}
                        for name, q in sorted(active.iteritems())]
        if indent_first:
            return text.indent("\n".join(info), indent)
        return info[0] + "\n" + text.indent("\n".join(info[1:]), indent)

    def select_subset(self, wanted, create_missing=True):
        """Sets :attr:`consume_from` by selecting a subset of the
        currently defined queues.

        :param wanted: List of wanted queue names.
        :keyword create_missing: By default any unknown queues will be
                                 added automatically, but if disabled
                                 the occurrence of unknown queues
                                 in `wanted` will raise :exc:`KeyError`.

        """
        if wanted:
            acc = {}
            for queue in wanted:
                try:
                    Q = self[queue]
                except KeyError:
                    if not create_missing:
                        raise
                    Q = self.new_missing(queue)
                acc[queue] = Q
            self._consume_from = acc
            self.update(acc)

    def new_missing(self, name):
        return Queue(name, Exchange(name), name)

    @property
    def consume_from(self):
        if self._consume_from is not None:
            return self._consume_from
        return self

    @classmethod
    def with_defaults(cls, queues, default_exchange):
        """Alternate constructor that adds default exchange and
        exchange type information to queues that does not have any."""
        queues = cls(queues if queues is not None else {})
        for q in queues.itervalues():
            if not q.exchange or not q.exchange.name:
                q.exchange = default_exchange
            if not q.routing_key:
                q.routing_key = default_exchange.name
        return queues


class TaskProducer(Producer):
    auto_declare = False
    retry = False
    retry_policy = None

    def __init__(self, channel=None, exchange=None, *args, **kwargs):
        self.app = kwargs.get("app") or self.app
        self.retry = kwargs.pop("retry", self.retry)
        self.retry_policy = kwargs.pop("retry_policy",
                                        self.retry_policy or {})
        exchange = exchange or self.exchange
        if not isinstance(exchange, Exchange):
            exchange = Exchange(exchange,
                    kwargs.get("exchange_type") or self.exchange_type)
        super(TaskProducer, self).__init__(channel, exchange, *args, **kwargs)

    def delay_task(self, task_name, task_args=None, task_kwargs=None,
            countdown=None, eta=None, task_id=None, taskset_id=None,
            expires=None, exchange=None, exchange_type=None,
            event_dispatcher=None, retry=None, retry_policy=None,
            queue=None, now=None, retries=0, chord=None, callbacks=None,
            errbacks=None, mandatory=None, priority=None, immediate=None,
            routing_key=None, serializer=None, delivery_mode=None,
            compression=None, **kwargs):
        """Send task message."""

        connection = self.connection
        _retry_policy = self.retry_policy
        if retry_policy:  # merge default and custom policy
            _retry_policy = dict(_retry_policy, **retry_policy)
        task_id = task_id or uuid()
        task_args = task_args or []
        task_kwargs = task_kwargs or {}
        if not isinstance(task_args, (list, tuple)):
            raise ValueError("task args must be a list or tuple")
        if not isinstance(task_kwargs, dict):
            raise ValueError("task kwargs must be a dictionary")
        if countdown:                           # Convert countdown to ETA.
            now = now or self.app.now()
            eta = now + timedelta(seconds=countdown)
        if isinstance(expires, (int, float)):
            now = now or self.app.now()
            expires = now + timedelta(seconds=expires)
        eta = eta and eta.isoformat()
        expires = expires and expires.isoformat()

        body = {"task": task_name,
                "id": task_id,
                "args": task_args or [],
                "kwargs": task_kwargs or {},
                "retries": retries or 0,
                "eta": eta,
                "expires": expires,
                "utc": self.utc,
                "callbacks": callbacks,
                "errbacks": errbacks}
        if taskset_id:
            body["taskset"] = taskset_id
        if chord:
            body["chord"] = chord

        self.publish(body, exchange=exchange, mandatory=mandatory,
             immediate=immediate, routing_key=routing_key,
             serializer=serializer or self.serializer,
             delivery_mode=delivery_mode,
             compression=compression or self.compression,
             retry=retry, retry_policy=retry_policy,
             declare=[self.app.amqp.queues[queue]] if queue else [])

        signals.task_sent.send(sender=task_name, **body)
        if event_dispatcher:
            event_dispatcher.send("task-sent", uuid=task_id,
                                               name=task_name,
                                               args=repr(task_args),
                                               kwargs=repr(task_kwargs),
                                               retries=retries,
                                               eta=eta,
                                               expires=expires)
        return task_id
TaskPublisher = TaskProducer  # compat


class TaskConsumer(Consumer):
    app = None

    def __init__(self, channel, queues=None, app=None, **kw):
        self.app = app or self.app
        super(TaskConsumer, self).__init__(channel,
                queues or self.app.amqp.queues.consume_from.values(), **kw)


class AMQP(object):
    BrokerConnection = BrokerConnection
    Consumer = Consumer

    #: Cached and prepared routing table.
    _rtable = None

    def __init__(self, app):
        self.app = app

    def flush_routes(self):
        self._rtable = _routes.prepare(self.app.conf.CELERY_ROUTES)

    def Queues(self, queues):
        """Create new :class:`Queues` instance, using queue defaults
        from the current configuration."""
        conf = self.app.conf
        if not queues and conf.CELERY_DEFAULT_QUEUE:
            queues = (Queue(conf.CELERY_DEFAULT_QUEUE,
                            exchange=self.default_exchange,
                            routing_key=conf.CELERY_DEFAULT_ROUTING_KEY), )
        return Queues.with_defaults(queues, self.default_exchange)

    def Router(self, queues=None, create_missing=None):
        """Returns the current task router."""
        return _routes.Router(self.routes, queues or self.queues,
                              self.app.either("CELERY_CREATE_MISSING_QUEUES",
                                              create_missing), app=self.app)

    @cached_property
    def TaskConsumer(self):
        """Returns consumer for a single task queue."""
        return self.app.subclass_with_self(TaskConsumer,
                reverse="amqp.TaskConsumer")

    def queue_or_default(self, q):
        if q:
            return self.queues[q] if not isinstance(q, Queue) else q
        return self.default_queue

    @cached_property
    def TaskProducer(self):
        """Returns publisher used to send tasks.

        You should use `app.send_task` instead.

        """
        conf = self.app.conf
        return self.app.subclass_with_self(TaskProducer,
                reverse="amqp.TaskProducer",
                exchange=self.default_exchange,
                exchange_type=self.default_exchange.type,
                routing_key=conf.CELERY_DEFAULT_ROUTING_KEY,
                serializer=conf.CELERY_TASK_SERIALIZER,
                compression=conf.CELERY_MESSAGE_COMPRESSION,
                retry=conf.CELERY_TASK_PUBLISH_RETRY,
                retry_policy=conf.CELERY_TASK_PUBLISH_RETRY_POLICY,
                utc=conf.CELERY_ENABLE_UTC)
    TaskPublisher = TaskProducer  # compat

    def get_task_consumer(self, channel, *args, **kwargs):
        """Return consumer configured to consume from all known task
        queues."""
        return self.TaskConsumer(channel, *args, **kwargs)

    @cached_property
    def default_queue(self):
        return self.queues[self.app.conf.CELERY_DEFAULT_QUEUE]

    @cached_property
    def queues(self):
        """Queue name⇒ declaration mapping."""
        return self.Queues(self.app.conf.CELERY_QUEUES)

    @queues.setter
    def queues(self, queues):  # noqa
        return self.Queues(queues)

    @property
    def routes(self):
        if self._rtable is None:
            self.flush_routes()
        return self._rtable

    @cached_property
    def router(self):
        return self.Router()

    @cached_property
    def publisher_pool(self):
        return ProducerPool(self.app.pool, limit=self.app.pool.limit,
                            Producer=self.TaskProducer)

    @cached_property
    def default_exchange(self):
        return Exchange(self.app.conf.CELERY_DEFAULT_EXCHANGE,
                        self.app.conf.CELERY_DEFAULT_EXCHANGE_TYPE)
