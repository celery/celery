"""

Sending and Receiving Messages

"""

from carrot.connection import DjangoBrokerConnection, AMQPConnectionException
from carrot.messaging import Publisher, Consumer, ConsumerSet
from billiard.utils.functional import wraps

from celery import conf
from celery import signals
from celery.utils import gen_unique_id, mitemgetter, noop, textindent

ROUTE_INFO_FORMAT = """
. %(name)s -> exchange:%(exchange)s (%(exchange_type)s) \
binding:%(binding_key)s
"""


MSG_OPTIONS = ("mandatory", "priority",
               "immediate", "routing_key",
               "serializer")

get_msg_options = mitemgetter(*MSG_OPTIONS)
extract_msg_options = lambda d: dict(zip(MSG_OPTIONS, get_msg_options(d)))


def routing_table():

    def _defaults(opts):
        opts.setdefault("exchange", conf.DEFAULT_EXCHANGE),
        opts.setdefault("exchange_type", conf.DEFAULT_EXCHANGE_TYPE)
        opts.setdefault("binding_key", "")
        return opts

    return dict((queue, _defaults(opts))
                    for queue, opts in conf.QUEUES.items())
_default_queue = routing_table()[conf.DEFAULT_QUEUE]


class TaskPublisher(Publisher):
    """The AMQP Task Publisher class."""
    exchange = _default_queue["exchange"]
    exchange_type = _default_queue["exchange_type"]
    routing_key = conf.DEFAULT_ROUTING_KEY
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


def get_consumer_set(connection, queues=None, **options):
    queues = queues or routing_table()
    return ConsumerSet(connection, from_dict=queues, **options)


class TaskConsumer(Consumer):
    """The AMQP Task Consumer class."""
    queue = conf.DEFAULT_QUEUE
    exchange = _default_queue["exchange"]
    routing_key = _default_queue["binding_key"]
    exchange_type = _default_queue["exchange_type"]
    auto_ack = False
    no_ack = False


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
    exchange = "celeryctl"
    exchange_type = "fanout"
    routing_key = ""

    def send(self, type, arguments, destination=None):
        arguments["command"] = type
        arguments["destination"] = destination
        super(BroadcastPublisher, self).send({"control": arguments})


class BroadcastConsumer(Consumer):
    queue = "celeryctl"
    exchange = "celeryctl"
    routing_key = ""
    exchange_type = "fanout"
    no_ack = True


def establish_connection(connect_timeout=conf.BROKER_CONNECTION_TIMEOUT):
    return DjangoBrokerConnection(connect_timeout=connect_timeout)


def with_connection(fun):

    @wraps(fun)
    def _inner(*args, **kwargs):
        connection = kwargs.get("connection")
        timeout = kwargs.get("connect_timeout",
                                conf.BROKER_CONNECTION_TIMEOUT)
        kwargs["connection"] = conn = connection or \
                establish_connection(connect_timeout=timeout)
        close_connection = not connection and conn.close or noop

        try:
            return fun(*args, **kwargs)
        finally:
            close_connection()
    return _inner


def with_connection_inline(fun, connection=None,
        connect_timeout=conf.BROKER_CONNECTION_TIMEOUT):
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


def format_routing_table(table=None, indent=0):
    table = table or routing_table()
    format = lambda **route: ROUTE_INFO_FORMAT.strip() % route
    routes = "\n".join(format(name=name, **route)
                            for name, route in table.items())
    return textindent(routes, indent=indent)
