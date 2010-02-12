"""

Sending and Receiving Messages

"""
import socket

from carrot.connection import DjangoBrokerConnection
from carrot.messaging import Publisher, Consumer, ConsumerSet
from billiard.utils.functional import wraps

from celery import conf
from celery import signals
from celery.utils import gen_unique_id, mitemgetter, noop


MSG_OPTIONS = ("mandatory", "priority",
               "immediate", "routing_key",
               "serializer")

get_msg_options = mitemgetter(*MSG_OPTIONS)
extract_msg_options = lambda d: dict(zip(MSG_OPTIONS, get_msg_options(d)))
default_queue = conf.routing_table[conf.DEFAULT_QUEUE]

_queues_declared = False


class TaskPublisher(Publisher):
    """Publish tasks."""
    exchange = default_queue["exchange"]
    exchange_type = default_queue["exchange_type"]
    routing_key = conf.DEFAULT_ROUTING_KEY
    serializer = conf.TASK_SERIALIZER

    def __init__(self, *args, **kwargs):
        super(TaskPublisher, self).__init__(*args, **kwargs)

        # Make sure all queues are declared.
        global _queues_declared
        if not _queues_declared:
            consumers = get_consumer_set(self.connection)
            consumers.close()
            _queues_declared = True

    def delay_task(self, task_name, task_args=None, task_kwargs=None,
            task_id=None, taskset_id=None, **kwargs):
        """Delay task for execution by the celery nodes."""

        task_id = task_id or gen_unique_id()
        eta = kwargs.get("eta")
        eta = eta and eta.isoformat()

        message_data = {
            "task": task_name,
            "id": task_id,
            "args": task_args or [],
            "kwargs": task_kwargs or {},
            "retries": kwargs.get("retries", 0),
            "eta": eta,
        }

        if taskset_id:
            message_data["taskset"] = taskset_id

        self.send(message_data, **extract_msg_options(kwargs))
        signals.task_sent.send(sender=task_name, **message_data)

        return task_id


class TaskConsumer(Consumer):
    """Consume tasks"""
    queue = conf.DEFAULT_QUEUE
    exchange = default_queue["exchange"]
    routing_key = default_queue["binding_key"]
    exchange_type = default_queue["exchange_type"]


class EventPublisher(Publisher):
    """Publish events"""
    exchange = conf.EVENT_EXCHANGE
    exchange_type = conf.EVENT_EXCHANGE_TYPE
    routing_key = conf.EVENT_ROUTING_KEY


class EventConsumer(Consumer):
    """Consume events"""
    queue = conf.EVENT_QUEUE
    exchange = conf.EVENT_EXCHANGE
    exchange_type = conf.EVENT_EXCHANGE_TYPE
    routing_key = conf.EVENT_ROUTING_KEY
    no_ack = True


class BroadcastPublisher(Publisher):
    """Publish broadcast commands"""
    exchange = conf.BROADCAST_EXCHANGE
    exchange_type = conf.BROADCAST_EXCHANGE_TYPE

    def send(self, type, arguments, destination=None):
        """Send broadcast command."""
        arguments["command"] = type
        arguments["destination"] = destination
        super(BroadcastPublisher, self).send({"control": arguments})


class BroadcastConsumer(Consumer):
    """Consume broadcast commands"""
    queue = conf.BROADCAST_QUEUE
    exchange = conf.BROADCAST_EXCHANGE
    exchange_type = conf.BROADCAST_EXCHANGE_TYPE
    no_ack = True

    def __init__(self, *args, **kwargs):
        hostname = kwargs.pop("hostname", None) or socket.gethostname()
        self.queue = "%s_%s" % (self.queue, hostname)
        super(BroadcastConsumer, self).__init__(*args, **kwargs)


def establish_connection(connect_timeout=conf.BROKER_CONNECTION_TIMEOUT):
    """Establish a connection to the message broker."""
    return DjangoBrokerConnection(connect_timeout=connect_timeout)


def with_connection(fun):
    """Decorator for providing default message broker connection for functions
    supporting the ``connection`` and ``connect_timeout`` keyword
    arguments."""

    @wraps(fun)
    def _inner(*args, **kwargs):
        connection = kwargs.get("connection")
        timeout = kwargs.get("connect_timeout", conf.BROKER_CONNECTION_TIMEOUT)
        kwargs["connection"] = conn = connection or \
                establish_connection(connect_timeout=timeout)
        close_connection = not connection and conn.close or noop

        try:
            return fun(*args, **kwargs)
        finally:
            close_connection()

    return _inner


def get_consumer_set(connection, queues=None, **options):
    """Get the :class:`carrot.messaging.ConsumerSet`` for a queue
    configuration.

    Defaults to the queues in ``CELERY_QUEUES``.

    """
    queues = queues or conf.routing_table
    cset = ConsumerSet(connection)
    for queue_name, queue_options in queues.items():
        queue_options = dict(queue_options)
        queue_options["routing_key"] = queue_options.pop("binding_key", None)
        consumer = Consumer(connection, queue=queue_name,
                            backend=cset.backend, **queue_options)
        cset.consumers.append(consumer)
    return cset
