"""

Sending and Receiving Messages

"""
from datetime import datetime, timedelta

from carrot.messaging import Publisher, Consumer, ConsumerSet as _ConsumerSet

from celery import signals
from celery.app import app_or_default, default_app
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


def establish_connection(**kwargs):
    """Establish a connection to the message broker."""
    app = app_or_default(kwargs.pop("app", None))
    return app.broker_connection(**kwargs)


def with_connection(fun):
    """Decorator for providing default message broker connection for functions
    supporting the ``connection`` and ``connect_timeout`` keyword
    arguments."""
    return default_app.with_default_connection(fun)


def get_consumer_set(connection, queues=None, **options):
    """Get the :class:`carrot.messaging.ConsumerSet`` for a queue
    configuration.

    Defaults to the queues in ``CELERY_QUEUES``.

    """
    return default_app.get_consumer_set(connection, queues, **options)
