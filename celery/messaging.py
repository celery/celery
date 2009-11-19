"""

Sending and Receiving Messages

"""
from carrot.connection import DjangoBrokerConnection
from carrot.messaging import Publisher, Consumer, ConsumerSet

from celery import conf
from celery import signals
from celery.utils import gen_unique_id
from celery.utils import mitemgetter

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
    auto_ack = False
    no_ack = False


class StatsPublisher(Publisher):
    exchange = "celerygraph"
    exchange_type = "direct"
    routing_key = "stats"


class StatsConsumer(Consumer):
    queue = "celerygraph"
    exchange = "celerygraph"
    routing_key = "stats"
    exchange_type = "direct"
    no_ack = True


class EventPublisher(Publisher):
    exchange = "celeryevent"
    routing_key = "event"


class EventConsumer(Consumer):
    queue = "celeryevent"
    exchange = "celeryevent"
    routing_key = "event"
    exchange_type = "direct"
    no_ack = True


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
    no_ack = True


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


def get_connection_info():
    broker_connection = DjangoBrokerConnection()
    carrot_backend = broker_connection.backend_cls
    if carrot_backend and not isinstance(carrot_backend, str):
        carrot_backend = carrot_backend.__name__
    port = broker_connection.port or \
                broker_connection.get_backend_cls().default_port
    port = port and ":%s" % port or ""
    vhost = broker_connection.virtual_host
    if not vhost.startswith("/"):
        vhost = "/" + vhost
    return "%(carrot_backend)s://%(userid)s@%(host)s%(port)s%(vhost)s" % {
                "carrot_backend": carrot_backend,
                "userid": broker_connection.userid,
                "host": broker_connection.hostname,
                "port": port,
                "vhost": vhost}


