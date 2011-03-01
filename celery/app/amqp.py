# -*- coding: utf-8 -*-
"""
celery.app.amqp
===============

AMQ related functionality.

:copyright: (c) 2009 - 2011 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from datetime import datetime, timedelta

from kombu import BrokerConnection, Exchange
from kombu.connection import Resource
from kombu import compat as messaging
from kombu.utils import cached_property

from celery import routes as _routes
from celery import signals
from celery.utils import gen_unique_id, textindent
from celery.utils import promise, maybe_promise
from celery.utils.compat import UserDict

#: List of known options to a Kombu producers send method.
#: Used to extract the message related options out of any `dict`.
MSG_OPTIONS = ("mandatory", "priority", "immediate", "routing_key",
                "serializer", "delivery_mode", "compression")

#: Human readable queue declaration.
QUEUE_FORMAT = """
. %(name)s exchange:%(exchange)s (%(exchange_type)s) \
binding:%(binding_key)s
"""

#: Set of exchange names that have already been declared.
_exchanges_declared = set()

#: Set of queue names that have already been declared.
_queues_declared = set()


def extract_msg_options(options, keep=MSG_OPTIONS):
    """Extracts known options to `basic_publish` from a dict,
    and returns a new dict."""
    return dict((name, options.get(name)) for name in keep)


class Queues(UserDict):
    """Queue name⇒ declaration mapping.

    Celery will consult this mapping to find the options
    for any queue by name.

    :param queues: Initial mapping.

    """
    #: If set, this is a subset of queues to consume from.
    #: The rest of the queues are then used for routing only.
    _consume_from = None

    def __init__(self, queues):
        self.data = {}
        for queue_name, options in (queues or {}).items():
            self.add(queue_name, **options)

    def add(self, queue, exchange=None, routing_key=None,
            exchange_type="direct", **options):
        """Add new queue.

        :param queue: Name of the queue.
        :keyword exchange: Name of the exchange.
        :keyword routing_key: Binding key.
        :keyword exchange_type: Type of exchange.
        :keyword \*\*options: Additional declaration options.

        """
        q = self[queue] = self.options(exchange, routing_key,
                                       exchange_type, **options)
        return q

    def options(self, exchange, routing_key,
            exchange_type="direct", **options):
        """Creates new option mapping for queue, with required
        keys present."""
        return dict(options, routing_key=routing_key,
                             binding_key=routing_key,
                             exchange=exchange,
                             exchange_type=exchange_type)

    def format(self, indent=0, indent_first=True):
        """Format routing table into string for log dumps."""
        queues = self
        if self._consume_from is not None:
            queues = self._consume_from
        info = [QUEUE_FORMAT.strip() % dict(
                    name=(name + ":").ljust(12), **config)
                        for name, config in sorted(queues.items())]
        if indent_first:
            return textindent("\n".join(info), indent)
        return info[0] + "\n" + textindent("\n".join(info[1:]), indent)

    def select_subset(self, wanted, create_missing=True):
        """Select subset of the currently defined queues.

        Does not return anything: queues not in `wanted` will
        be discarded in-place.

        :param wanted: List of wanted queue names.
        :keyword create_missing: By default any unknown queues will be
                                 added automatically, but if disabled
                                 the occurrence of unknown queues
                                 in `wanted` will raise :exc:`KeyError`.

        """
        acc = {}
        for queue in wanted:
            try:
                options = self[queue]
            except KeyError:
                if not create_missing:
                    raise
                options = self.options(queue, queue)
            acc[queue] = options
        self._consume_from = acc
        self.update(acc)

    @classmethod
    def with_defaults(cls, queues, default_exchange, default_exchange_type):
        """Alternate constructor that adds default exchange and
        exchange type information to queues that does not have any."""
        for opts in queues.values():
            opts.setdefault("exchange", default_exchange),
            opts.setdefault("exchange_type", default_exchange_type)
            opts.setdefault("binding_key", default_exchange)
            opts.setdefault("routing_key", opts.get("binding_key"))
        return cls(queues)

    @property
    def consume_from(self):
        if self._consume_from is not None:
            return self._consume_from
        return self


class TaskPublisher(messaging.Publisher):
    auto_declare = True
    retry = False
    retry_policy = None

    def __init__(self, *args, **kwargs):
        self.app = kwargs.pop("app")
        self.retry = kwargs.pop("retry", self.retry)
        self.retry_policy = kwargs.pop("retry_policy",
                                        self.retry_policy or {})
        super(TaskPublisher, self).__init__(*args, **kwargs)

    def declare(self):
        if self.exchange.name and \
                self.exchange.name not in _exchanges_declared:
            super(TaskPublisher, self).declare()
            _exchanges_declared.add(self.exchange.name)

    def _declare_queue(self, name, retry=False, retry_policy={}):
        options = self.app.queues[name]
        queue = messaging.entry_to_queue(name, **options)(self.channel)
        if retry:
            self.connection.ensure(queue, queue.declare, **retry_policy)()
        else:
            queue.declare()
        return queue

    def _declare_exchange(self, name, type, retry=False, retry_policy={}):
        ex = Exchange(name, type=type, durable=self.durable,
                      auto_delete=self.auto_delete)(self.channel)
        if retry:
            return self.connection.ensure(ex, ex.declare, **retry_policy)
        return ex.declare()

    def delay_task(self, task_name, task_args=None, task_kwargs=None,
            countdown=None, eta=None, task_id=None, taskset_id=None,
            expires=None, exchange=None, exchange_type=None,
            event_dispatcher=None, retry=None, retry_policy=None,
            queue=None, now=None, retries=0, **kwargs):
        """Send task message."""
        connection = self.connection
        _retry_policy = self.retry_policy
        if retry_policy:  # merge default and custom policy
            _retry_policy = dict(_retry_policy, **retry_policy)

        # declare entities
        if queue and queue not in _queues_declared:
            entity = self._declare_queue(queue, retry, _retry_policy)
            _exchanges_declared.add(entity.exchange.name)
            _queues_declared.add(entity.name)
        if exchange and exchange not in _exchanges_declared:
            self._declare_exchange(exchange,
                    exchange_type or self.exchange_type, retry, _retry_policy)
            _exchanges_declared.add(exchange)

        task_id = task_id or gen_unique_id()
        task_args = task_args or []
        task_kwargs = task_kwargs or {}
        if not isinstance(task_args, (list, tuple)):
            raise ValueError("task args must be a list or tuple")
        if not isinstance(task_kwargs, dict):
            raise ValueError("task kwargs must be a dictionary")
        if countdown:                           # Convert countdown to ETA.
            now = now or datetime.now()
            eta = now + timedelta(seconds=countdown)
        if isinstance(expires, int):
            now = now or datetime.now()
            expires = now + timedelta(seconds=expires)
        eta = eta and eta.isoformat()
        expires = expires and expires.isoformat()

        body = {"task": task_name,
                "id": task_id,
                "args": task_args or [],
                "kwargs": task_kwargs or {},
                "retries": retries or 0,
                "eta": eta,
                "expires": expires}
        if taskset_id:
            body["taskset"] = taskset_id

        send = self.send
        if retry is None and self.retry or retry:
            send = connection.ensure(self, self.send, **_retry_policy)
        send(body, exchange=exchange, **extract_msg_options(kwargs))
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

    def __exit__(self, *exc_info):
        try:
            self.release()
        except AttributeError:
            self.close()


class PublisherPool(Resource):

    def __init__(self, limit=None, app=None):
        self.app = app
        self.connections = self.app.broker_connection().Pool(limit=limit)
        super(PublisherPool, self).__init__(limit=limit)

    def create_publisher(self):
        return self.app.amqp.TaskPublisher(self.connections.acquire(),
                                           auto_declare=False)

    def new(self):
        return promise(self.create_publisher)

    def setup(self):
        if self.limit:
            for _ in xrange(self.limit):
                self._resource.put_nowait(self.new())

    def prepare(self, publisher):
        return maybe_promise(publisher)


class AMQP(object):
    BrokerConnection = BrokerConnection
    Publisher = messaging.Publisher
    Consumer = messaging.Consumer
    ConsumerSet = messaging.ConsumerSet

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
        if not queues:
            queues = {conf.CELERY_DEFAULT_QUEUE: {
                        "exchange": conf.CELERY_DEFAULT_EXCHANGE,
                        "exchange_type": conf.CELERY_DEFAULT_EXCHANGE_TYPE,
                        "binding_key": conf.CELERY_DEFAULT_ROUTING_KEY}}
        return Queues.with_defaults(queues, conf.CELERY_DEFAULT_EXCHANGE,
                                            conf.CELERY_DEFAULT_EXCHANGE_TYPE)

    def Router(self, queues=None, create_missing=None):
        """Returns the current task router."""
        return _routes.Router(self.routes, queues or self.queues,
                              self.app.either("CELERY_CREATE_MISSING_QUEUES",
                                              create_missing), app=self.app)

    def TaskConsumer(self, *args, **kwargs):
        """Returns consumer for a single task queue."""
        default_queue_name, default_queue = self.get_default_queue()
        defaults = dict({"queue": default_queue_name}, **default_queue)
        defaults["routing_key"] = defaults.pop("binding_key", None)
        return self.Consumer(*args,
                             **self.app.merge(defaults, kwargs))

    def TaskPublisher(self, *args, **kwargs):
        """Returns publisher used to send tasks.

        You should use `app.send_task` instead.

        """
        conf = self.app.conf
        _, default_queue = self.get_default_queue()
        defaults = {"exchange": default_queue["exchange"],
                    "exchange_type": default_queue["exchange_type"],
                    "routing_key": conf.CELERY_DEFAULT_ROUTING_KEY,
                    "serializer": conf.CELERY_TASK_SERIALIZER,
                    "retry": conf.CELERY_TASK_PUBLISH_RETRY,
                    "retry_policy": conf.CELERY_TASK_PUBLISH_RETRY_POLICY,
                    "app": self}
        return TaskPublisher(*args, **self.app.merge(defaults, kwargs))

    def PublisherPool(self, limit=None):
        return PublisherPool(limit=limit, app=self.app)

    def get_task_consumer(self, connection, queues=None, **kwargs):
        """Return consumer configured to consume from all known task
        queues."""
        return self.ConsumerSet(connection,
                                from_dict=queues or self.queues.consume_from,
                                **kwargs)

    def get_default_queue(self):
        """Returns `(queue_name, queue_options)` tuple for the queue
        configured to be default (:setting:`CELERY_DEFAULT_QUEUE`)."""
        q = self.app.conf.CELERY_DEFAULT_QUEUE
        return q, self.queues[q]

    @cached_property
    def queues(self):
        """Queue name⇒ declaration mapping."""
        return self.Queues(self.app.conf.CELERY_QUEUES)

    @property
    def routes(self):
        if self._rtable is None:
            self.flush_routes()
        return self._rtable
