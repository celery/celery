"""

Sending and Receiving Messages

"""
import socket
import warnings

from datetime import datetime, timedelta
from itertools import count

from carrot.connection import BrokerConnection
from carrot.messaging import Publisher, Consumer, ConsumerSet as _ConsumerSet

from celery import signals
from celery.defaults import app_or_default, default_app
from celery.utils import gen_unique_id, mitemgetter, noop
from celery.utils.functional import wraps


MSG_OPTIONS = ("mandatory", "priority", "immediate",
               "routing_key", "serializer", "delivery_mode")

get_msg_options = mitemgetter(*MSG_OPTIONS)
extract_msg_options = lambda d: dict(zip(MSG_OPTIONS, get_msg_options(d)))

_queues_declared = False
_exchanges_declared = set()


class TaskPublisher(Publisher):
    """Publish tasks."""
    auto_declare = False

    def __init__(self, *args, **kwargs):
        self.app = app = app_or_default(kwargs.get("app"))
        _, default_queue = app.get_default_queue()
        kwargs["exchange"] = kwargs.get("exchange") or \
                                    default_queue["exchange"]
        kwargs["exchange_type"] = kwargs.get("exchange_type") or \
                                    default_queue["exchange_type"]
        kwargs["routing_key"] = kwargs.get("routing_key") or \
                                    app.conf.CELERY_DEFAULT_ROUTING_KEY
        kwargs["serializer"] = kwargs.get("serializer") or \
                                    app.conf.CELERY_TASK_SERIALIZER
        super(TaskPublisher, self).__init__(*args, **kwargs)

        # Make sure all queues are declared.
        global _queues_declared
        if not _queues_declared:
            consumers = self.app.get_consumer_set(self.connection,
                                                  self.app.get_queues())
            consumers.close()
            _queues_declared = True
        self.declare()

    def declare(self):
        if self.exchange not in _exchanges_declared:
            super(TaskPublisher, self).declare()
            _exchanges_declared.add(self.exchange)

    def delay_task(self, task_name, task_args=None, task_kwargs=None,
            countdown=None, eta=None, task_id=None, taskset_id=None,
            expires=None, **kwargs):
        """Delay task for execution by the celery nodes."""

        task_id = task_id or gen_unique_id()
        task_args = task_args or []
        task_kwargs = task_kwargs or {}
        now = None
        if countdown: # Convert countdown to ETA.
            now = datetime.now()
            eta = now + timedelta(seconds=countdown)

        if not isinstance(task_args, (list, tuple)):
            raise ValueError("task args must be a list or tuple")
        if not isinstance(task_kwargs, dict):
            raise ValueError("task kwargs must be a dictionary")

        if isinstance(expires, int):
            now = now or datetime.now()
            expires = now + timedelta(seconds=expires)

        message_data = {
            "task": task_name,
            "id": task_id,
            "args": task_args or [],
            "kwargs": task_kwargs or {},
            "retries": kwargs.get("retries", 0),
            "eta": eta and eta.isoformat(),
            "expires": expires and expires.isoformat(),
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

    def __init__(self, *args, **kwargs):
        app = app_or_default(kwargs.get("app"))
        default_queue_name, default_queue = app.get_default_queue()
        kwargs["queue"] = kwargs.get("queue") or default_queue_name
        kwargs["exchange"] = kwargs.get("exchange") or \
                                default_queue["exchange"]
        kwargs["exchange_type"] = kwargs.get("exchange_type") or \
                                default_queue["exchange_type"]
        kwargs["routing_key"] = kwargs.get("routing_key") or \
                                    default_queue["binding_key"]
        super(TaskConsumer, self).__init__(*args, **kwargs)


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

    def collect(self, limit=None, timeout=1, callback=None):
        responses = []

        def on_message(message_data, message):
            if callback:
                callback(message_data)
            responses.append(message_data)

        self.callbacks = [on_message]
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
    durable = False
    auto_delete = True


class BroadcastPublisher(Publisher):
    """Publish broadcast commands"""

    ReplyTo = ControlReplyConsumer

    def __init__(self, *args, **kwargs):
        app = self.app = app_or_default(kwargs.get("app"))
        kwargs["exchange"] = kwargs.get("exchange") or \
                                app.conf.CELERY_BROADCAST_EXCHANGE
        kwargs["exchange_type"] = kwargs.get("exchange_type") or \
                                app.conf.CELERY_BROADCAST_EXCHANGE_TYPE
        super(BroadcastPublisher, self).__init__(*args, **kwargs)

    def send(self, type, arguments, destination=None, reply_ticket=None):
        """Send broadcast command."""
        arguments["command"] = type
        arguments["destination"] = destination
        reply_to = self.ReplyTo(self.connection, None, app=self.app,
                                auto_declare=False)
        if reply_ticket:
            arguments["reply_to"] = {"exchange": self.ReplyTo.exchange,
                                     "routing_key": reply_ticket}
        super(BroadcastPublisher, self).send({"control": arguments})


class BroadcastConsumer(Consumer):
    """Consume broadcast commands"""
    no_ack = True

    def __init__(self, *args, **kwargs):
        self.app = app = app_or_default(kwargs.get("app"))
        kwargs["queue"] = kwargs.get("queue") or \
                            app.conf.CELERY_BROADCAST_QUEUE
        kwargs["exchange"] = kwargs.get("exchange") or \
                            app.conf.CELERY_BROADCAST_EXCHANGE
        kwargs["exchange_type"] = kwargs.get("exchange_type") or \
                            app.conf.CELERY_BROADCAST_EXCHANGE_TYPE
        self.hostname = kwargs.pop("hostname", None) or socket.gethostname()
        self.queue = "%s_%s" % (self.queue, self.hostname)
        super(BroadcastConsumer, self).__init__(*args, **kwargs)

    def verify_exclusive(self):
        # XXX Kombu material
        channel = getattr(self.backend, "channel")
        if channel and hasattr(channel, "queue_declare"):
            try:
                _, _, consumers = channel.queue_declare(self.queue,
                                                        passive=True)
            except ValueError:
                pass
            else:
                if consumers:
                    warnings.warn(UserWarning(
                        "A node named %s is already using this process "
                        "mailbox. Maybe you should specify a custom name "
                        "for this node with the -n argument?" % self.hostname))

    def consume(self, *args, **kwargs):
        self.verify_exclusive()
        return super(BroadcastConsumer, self).consume(*args, **kwargs)


def establish_connection(hostname=None, userid=None, password=None,
        virtual_host=None, port=None, ssl=None, insist=None,
        connect_timeout=None, backend_cls=None, app=None):
    """Establish a connection to the message broker."""
    app = app_or_default(app)
    if insist is None:
        insist = app.conf.get("BROKER_INSIST")
    if ssl is None:
        ssl = app.conf.get("BROKER_USE_SSL")
    if connect_timeout is None:
        connect_timeout = app.conf.get("BROKER_CONNECTION_TIMEOUT")

    return BrokerConnection(hostname or app.conf.BROKER_HOST,
                            userid or app.conf.BROKER_USER,
                            password or app.conf.BROKER_PASSWORD,
                            virtual_host or app.conf.BROKER_VHOST,
                            port or app.conf.BROKER_PORT,
                            backend_cls=backend_cls or app.conf.BROKER_BACKEND,
                            insist=insist, ssl=ssl,
                            connect_timeout=connect_timeout)


def with_connection(fun):
    """Decorator for providing default message broker connection for functions
    supporting the ``connection`` and ``connect_timeout`` keyword
    arguments."""

    @wraps(fun)
    def _inner(*args, **kwargs):
        connection = kwargs.get("connection")
        timeout = kwargs.get("connect_timeout",
                    default_app.conf.get("BROKER_CONNECTION_TIMEOUT"))
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
    return default_app.get_consumer_set(connection, queues, **options)
