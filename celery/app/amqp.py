# -*- coding: utf-8 -*-
"""
celery.app.amqp
===============

AMQ related functionality.

:copyright: (c) 2009 - 2010 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from datetime import datetime, timedelta

from kombu import BrokerConnection
from kombu import compat as messaging

from celery import routes
from celery import signals
from celery.utils import gen_unique_id, textindent, cached_property
from celery.utils.compat import UserDict

#: List of known options to a Kombu producers send method.
#: Used to extract the message related options out of any `dict`.
MSG_OPTIONS = ("mandatory", "priority", "immediate",
               "routing_key", "serializer", "delivery_mode",
               "compression")

#: Human readable queue declaration.
QUEUE_FORMAT = """
. %(name)s -> exchange:%(exchange)s (%(exchange_type)s) \
binding:%(binding_key)s
"""

#: Broker connection info -> URI
BROKER_FORMAT = """\
%(transport)s://%(userid)s@%(hostname)s%(port)s%(virtual_host)s\
"""

#: Set to :const:`True` when the configured queues has been declared.
_queues_declared = False

#: Set of exchange names that has already been declared.
_exchanges_declared = set()


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

    def format(self, indent=0):
        """Format routing table into string for log dumps."""
        info = "\n".join(QUEUE_FORMAT.strip() % dict(name=name, **config)
                                for name, config in self.items())
        return textindent(info, indent=indent)

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
        self.data.clear()
        self.data.update(acc)

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


class TaskPublisher(messaging.Publisher):
    auto_declare = False

    def declare(self):
        if self.exchange.name not in _exchanges_declared:
            super(TaskPublisher, self).declare()
            _exchanges_declared.add(self.exchange.name)

    def delay_task(self, task_name, task_args=None, task_kwargs=None,
            countdown=None, eta=None, task_id=None, taskset_id=None,
            expires=None, exchange=None, exchange_type=None,
            event_dispatcher=None, **kwargs):
        """Send task message."""

        task_id = task_id or gen_unique_id()
        task_args = task_args or []
        task_kwargs = task_kwargs or {}
        now = None
        if countdown:                           # Convert countdown to ETA.
            now = datetime.now()
            eta = now + timedelta(seconds=countdown)

        if not isinstance(task_args, (list, tuple)):
            raise ValueError("task args must be a list or tuple")
        if not isinstance(task_kwargs, dict):
            raise ValueError("task kwargs must be a dictionary")

        if isinstance(expires, int):
            now = now or datetime.now()
            expires = now + timedelta(seconds=expires)

        retries = kwargs.get("retries", 0)
        eta = eta and eta.isoformat()
        expires = expires and expires.isoformat()

        message_data = {
            "task": task_name,
            "id": task_id,
            "args": task_args or [],
            "kwargs": task_kwargs or {},
            "retries": retries,
            "eta": eta,
            "expires": expires,
        }

        if taskset_id:
            message_data["taskset"] = taskset_id

        # custom exchange passed, need to declare it.
        if exchange and exchange not in _exchanges_declared:
            exchange_type = exchange_type or self.exchange_type
            self.backend.exchange_declare(exchange=exchange,
                                          type=exchange_type,
                                          durable=self.durable,
                                          auto_delete=self.auto_delete)
        self.send(message_data, exchange=exchange,
                  **extract_msg_options(kwargs))
        signals.task_sent.send(sender=task_name, **message_data)

        if event_dispatcher:
            event_dispatcher.send("task-sent", uuid=task_id,
                                               name=task_name,
                                               args=repr(task_args),
                                               kwargs=repr(task_kwargs),
                                               retries=retries,
                                               eta=eta,
                                               expires=expires)
        return task_id


class AMQP(object):
    BrokerConnection = BrokerConnection
    Publisher = messaging.Publisher
    Consumer = messaging.Consumer
    ConsumerSet = messaging.ConsumerSet

    def __init__(self, app):
        self.app = app

    def Queues(self, queues):
        """Create new :class:`Queues` instance, using queue defaults
        from the current configuration."""
        return Queues.with_defaults(queues,
                                    self.app.conf.CELERY_DEFAULT_EXCHANGE,
                                    self.app.conf.CELERY_DEFAULT_EXCHANGE_TYPE)

    def Router(self, queues=None, create_missing=None):
        """Returns the current task router."""
        return routes.Router(self.app.conf.CELERY_ROUTES,
                             queues or self.app.conf.CELERY_QUEUES,
                             self.app.either("CELERY_CREATE_MISSING_QUEUES",
                                             create_missing),
                             app=self.app)

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
        _, default_queue = self.get_default_queue()
        defaults = {"exchange": default_queue["exchange"],
                    "exchange_type": default_queue["exchange_type"],
                    "routing_key": self.app.conf.CELERY_DEFAULT_ROUTING_KEY,
                    "serializer": self.app.conf.CELERY_TASK_SERIALIZER}
        publisher = TaskPublisher(*args,
                                  **self.app.merge(defaults, kwargs))

        # Make sure all queues are declared.
        global _queues_declared
        if not _queues_declared:
            self.get_task_consumer(publisher.connection).close()
            _queues_declared = True
        publisher.declare()

        return publisher

    def get_task_consumer(self, connection, queues=None, **kwargs):
        """Return consumer configured to consume from all known task
        queues."""
        return self.ConsumerSet(connection, from_dict=queues or self.queues,
                                **kwargs)

    def get_default_queue(self):
        """Returns `(queue_name, queue_options)` tuple for the queue
        configured to be default (:setting:`CELERY_DEFAULT_QUEUE`)."""
        q = self.app.conf.CELERY_DEFAULT_QUEUE
        return q, self.queues[q]

    def get_broker_info(self, broker_connection=None):
        """Returns information about the current broker connection
        as a `dict`."""
        if broker_connection is None:
            broker_connection = self.app.broker_connection()
        info = broker_connection.info()
        port = info["port"]
        if port:
            info["port"] = ":%s" % (port, )
        vhost = info["virtual_host"]
        if not vhost.startswith("/"):
            info["virtual_host"] = "/" + vhost
        return info

    def format_broker_info(self, info=None):
        """Get message broker connection info string for log dumps."""
        return BROKER_FORMAT % self.get_broker_info()

    @cached_property
    def queues(self):
        return self.Queues(self.app.conf.CELERY_QUEUES)

    @queues.setter
    def queues(self, value):
        return self.Queues(value)
