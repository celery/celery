"""

Sending and Receiving Messages

"""
import socket
from datetime import datetime, timedelta
from itertools import count

from carrot.connection import BrokerConnection
from carrot.messaging import Publisher, Consumer, ConsumerSet as _ConsumerSet

from celery import conf
from celery import signals
from celery.utils import gen_unique_id, mitemgetter, noop
from celery.utils.functional import wraps


MSG_OPTIONS = ("mandatory", "priority", "immediate",
               "routing_key", "serializer", "delivery_mode")

get_msg_options = mitemgetter(*MSG_OPTIONS)
extract_msg_options = lambda d: dict(zip(MSG_OPTIONS, get_msg_options(d)))
default_queue = conf.get_queues()[conf.DEFAULT_QUEUE]

_queues_declared = False
_exchanges_declared = {}


class TaskPublisher(Publisher):
    """Publish tasks."""
    exchange = default_queue["exchange"]
    exchange_type = default_queue["exchange_type"]
    routing_key = conf.DEFAULT_ROUTING_KEY
    serializer = conf.TASK_SERIALIZER
    auto_declare = False

    def __init__(self, *args, **kwargs):
        super(TaskPublisher, self).__init__(*args, **kwargs)

        # Make sure all queues are declared.
        global _queues_declared
        if not _queues_declared:
            consumers = get_consumer_set(self.connection)
            consumers.close()
            _queues_declared = True
        if self.exchange not in _exchanges_declared:
            self.declare()
            _exchanges_declared[self.exchange] = True

    def delay_task(self, task_name, task_args=None, task_kwargs=None,
            countdown=None, eta=None, task_id=None, taskset_id=None, **kwargs):
        """Delay task for execution by the celery nodes."""

        task_id = task_id or gen_unique_id()
        task_args = task_args or []
        task_kwargs = task_kwargs or {}
        if countdown: # Convert countdown to ETA.
            eta = datetime.now() + timedelta(seconds=countdown)

        if not isinstance(task_args, (list, tuple)):
            raise ValueError("task args must be a list or tuple")
        if not isinstance(task_kwargs, dict):
            raise ValueError("task kwargs must be a dictionary")

        message_data = {
            "task": task_name,
            "id": task_id,
            "args": task_args or [],
            "kwargs": task_kwargs or {},
            "retries": kwargs.get("retries", 0),
            "eta": eta and eta.isoformat(),
        }

        if taskset_id:
            message_data["taskset"] = taskset_id

        self.send(message_data, **extract_msg_options(kwargs))
        signals.task_sent.send(sender=task_name, **message_data)

        return task_id


class ConsumerSet(_ConsumerSet):
    """ConsumerSet with an optional decode error callback.

    For more information see :class:`carrot.messaging.ConsumerSet`.

    .. attribute:: on_decode_error

        Callback called if a message had decoding errors.
        The callback is called with the signature::

            callback(message, exception)

    """
    on_decode_error = None

    def _receive_callback(self, raw_message):
        message = self.backend.message_to_python(raw_message)
        if self.auto_ack and not message.acknowledged:
            message.ack()
        try:
            decoded = message.decode()
        except Exception, exc:
            if self.on_decode_error:
                return self.on_decode_error(message, exc)
            else:
                raise
        self.receive(decoded, message)


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
    serializer = conf.EVENT_SERIALIZER


class EventConsumer(Consumer):
    """Consume events"""
    queue = conf.EVENT_QUEUE
    exchange = conf.EVENT_EXCHANGE
    exchange_type = conf.EVENT_EXCHANGE_TYPE
    routing_key = conf.EVENT_ROUTING_KEY
    no_ack = True


class ControlReplyConsumer(Consumer):
    exchange = "celerycrq"
    exchange_type = "direct"
    durable = False
    exclusive = False
    auto_delete = True
    no_ack = True

    def __init__(self, connection, ticket, **kwargs):
        self.ticket = ticket
        queue = "%s.%s" % (self.exchange, ticket)
        super(ControlReplyConsumer, self).__init__(connection,
                                                   queue=queue,
                                                   routing_key=ticket,
                                                   **kwargs)

    def collect(self, limit=None, timeout=1):
        responses = []

        def callback(message_data, message):
            responses.append(message_data)

        self.callbacks = [callback]
        self.consume()
        for i in limit and range(limit) or count():
            try:
                self.connection.drain_events(timeout=timeout)
            except socket.timeout:
                break

        return responses


class ControlReplyPublisher(Publisher):
    exchange = "celerycrq"
    exchange_type = "direct"
    delivery_mode = "non-persistent"


class BroadcastPublisher(Publisher):
    """Publish broadcast commands"""

    ReplyTo = ControlReplyConsumer

    exchange = conf.BROADCAST_EXCHANGE
    exchange_type = conf.BROADCAST_EXCHANGE_TYPE

    def send(self, type, arguments, destination=None, reply_ticket=None):
        """Send broadcast command."""
        arguments["command"] = type
        arguments["destination"] = destination
        if reply_ticket:
            arguments["reply_to"] = {"exchange": self.ReplyTo.exchange,
                                     "routing_key": reply_ticket}
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


def establish_connection(hostname=None, userid=None, password=None,
        virtual_host=None, port=None, ssl=None, insist=None,
        connect_timeout=None, backend_cls=None):
    """Establish a connection to the message broker."""
    if insist is None:
        insist = conf.BROKER_INSIST
    if ssl is None:
        ssl = conf.BROKER_USE_SSL
    if connect_timeout is None:
        connect_timeout = conf.BROKER_CONNECTION_TIMEOUT

    return BrokerConnection(hostname or conf.BROKER_HOST,
                            userid or conf.BROKER_USER,
                            password or conf.BROKER_PASSWORD,
                            virtual_host or conf.BROKER_VHOST,
                            port or conf.BROKER_PORT,
                            backend_cls=backend_cls or conf.BROKER_BACKEND,
                            insist=insist, ssl=ssl,
                            connect_timeout=connect_timeout)


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
    queues = queues or conf.get_queues()
    cset = ConsumerSet(connection)
    for queue_name, queue_options in queues.items():
        queue_options = dict(queue_options)
        queue_options["routing_key"] = queue_options.pop("binding_key", None)
        consumer = Consumer(connection, queue=queue_name,
                            backend=cset.backend, **queue_options)
        cset.consumers.append(consumer)
    return cset


@with_connection
def reply(data, exchange, routing_key, connection=None, connect_timeout=None,
        **kwargs):
    pub = Publisher(connection, exchange=exchange,
                    routing_key=routing_key, **kwargs)
    pub.send(data)
