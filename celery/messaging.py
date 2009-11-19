"""

Sending and Receiving Messages

"""
from carrot.connection import DjangoBrokerConnection
from carrot.messaging import Publisher, Consumer, ConsumerSet

from celery import conf
from celery import signals
from celery.utils import gen_unique_id
from celery.utils import mitemgetter
from celery.serialization import pickle


MSG_OPTIONS = ("mandatory", "priority",
               "immediate", "routing_key",
               "serializer")

get_msg_options = mitemgetter(*MSG_OPTIONS)

extract_msg_options = lambda d: dict(zip(MSG_OPTIONS, get_msg_options(d)))


class TaskPublisher(Publisher):
    """The AMQP Task Publisher class."""
    exchange = conf.AMQP_EXCHANGE
    exchange_type = conf.AMQP_EXCHANGE_TYPE
    routing_key = conf.AMQP_PUBLISHER_ROUTING_KEY
    serializer = conf.TASK_SERIALIZER
    encoder = pickle.dumps

    def delay_task(self, task_name, task_args, task_kwargs, **kwargs):
        """Delay task for execution by the celery nodes."""
        return self._delay_task(task_name=task_name, task_args=task_args,
                                task_kwargs=task_kwargs, **kwargs)

    def _delay_task(self, task_name, task_id=None, taskset_id=None,
            task_args=None, task_kwargs=None, **kwargs):
        """INTERNAL"""

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


def get_consumer_set(connection, queues=conf.AMQP_CONSUMER_QUEUES, **options):
    return ConsumerSet(connection, from_dict=queues, **options)


class TaskConsumer(Consumer):
    """The AMQP Task Consumer class."""
    queue = conf.AMQP_CONSUMER_QUEUE
    exchange = conf.AMQP_EXCHANGE
    routing_key = conf.AMQP_CONSUMER_ROUTING_KEY
    exchange_type = conf.AMQP_EXCHANGE_TYPE
    decoder = pickle.loads
    auto_ack = False
    no_ack = False


class StatsPublisher(Publisher):
    exchange = "celerygraph"
    exchange_type = "direct"
    routing_key = "stats"
    encoder = pickle.dumps


class StatsConsumer(Consumer):
    queue = "celerygraph"
    exchange = "celerygraph"
    routing_key = "stats"
    exchange_type = "direct"
    decoder = pickle.loads
    no_ack=True


class BroadcastPublisher(Publisher):
    exchange = "celerycast"
    exchange_type = "fanout"
    routing_key = ""

    def revoke(self, task_id):
        self.send(dict(revoke=task_id))


class BroadcastConsumer(Consumer):
    queue = "celerycast"
    exchange = "celerycast"
    routing_key = ""
    exchange_type = "fanout"
    no_ack=True


def establish_connection(connect_timeout=conf.AMQP_CONNECTION_TIMEOUT):
    return DjangoBrokerConnection(connect_timeout=connect_timeout)


def with_connection(fun, connection=None,
        connect_timeout=conf.AMQP_CONNECTION_TIMEOUT):
    conn = connection or establish_connection()
    close_connection = not connection and conn.close or noop

    try:
        return fun(conn)
    finally:
        close_connection()
