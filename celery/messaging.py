"""

Sending and Receiving Messages

"""
from carrot.messaging import Publisher, Consumer, ConsumerSet
from celery import conf
from celery.utils import gen_unique_id
from celery.utils import mitemgetter
from celery.serialization import pickle


MSG_OPTIONS = ("mandatory", "priority",
               "immediate", "routing_key")

get_msg_options = mitemgetter(*MSG_OPTIONS)

extract_msg_options = lambda d: dict(zip(MSG_OPTIONS, get_msg_options(d)))


class TaskPublisher(Publisher):
    """The AMQP Task Publisher class."""
    exchange = conf.AMQP_EXCHANGE
    exchange_type = conf.AMQP_EXCHANGE_TYPE
    routing_key = conf.AMQP_PUBLISHER_ROUTING_KEY
    serializer = "pickle"
    encoder = pickle.dumps

    def delay_task(self, task_name, task_args, task_kwargs, **kwargs):
        """Delay task for execution by the celery nodes."""
        return self._delay_task(task_name=task_name, task_args=task_args,
                                task_kwargs=task_kwargs, **kwargs)

    def delay_task_in_set(self, taskset_id, task_name, task_args, task_kwargs,
            **kwargs):
        """Delay a task which part of a task set."""
        return self._delay_task(task_name=task_name, part_of_set=taskset_id,
                                task_args=task_args, task_kwargs=task_kwargs,
                                **kwargs)

    def retry_task(self, task_name, task_id, delivery_info, **kwargs):
        kwargs["routing_key"] = delivery_info.get("routing_key")
        kwargs["retries"] = kwargs.get("retries", 0) + 1
        self._delay_task(task_name, task_id, **kwargs)

    def _delay_task(self, task_name, task_id=None, part_of_set=None,
            task_args=None, task_kwargs=None, **kwargs):
        """INTERNAL"""

        task_id = task_id or gen_unique_id()

        message_data = {
            "task": task_name,
            "id": task_id,
            "args": task_args or [],
            "kwargs": task_kwargs or {},
            "retries": kwargs.get("retries", 0),
            "eta": kwargs.get("eta"),
        }

        if part_of_set:
            message_data["taskset"] = part_of_set

        self.send(message_data, **extract_msg_options(kwargs))
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
    routing_key = "stats"
    encoder = pickle.dumps


class StatsConsumer(Consumer):
    queue = "celerygraph"
    exchange = "celerygraph"
    routing_key = "stats"
    exchange_type = "direct"
    decoder = pickle.loads
    no_ack=True
