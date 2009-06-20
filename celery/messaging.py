"""

Sending and Receiving Messages

"""
from carrot.messaging import Publisher, Consumer, ConsumerSet
from celery import conf
import uuid

try:
    import cPickle as pickle
except ImportError:
    import pickle


class TaskPublisher(Publisher):
    """The AMQP Task Publisher class."""
    exchange = conf.AMQP_EXCHANGE
    exchange_type = conf.AMQP_EXCHANGE_TYPE
    routing_key = conf.AMQP_PUBLISHER_ROUTING_KEY
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

    def requeue_task(self, task_name, task_id, task_args, task_kwargs,
            part_of_set=None, **kwargs):
        """Requeue a failed task."""
        return self._delay_task(task_name=task_name, part_of_set=part_of_set,
                                task_id=task_id, task_args=task_args,
                                task_kwargs=task_kwargs, **kwargs)

    def _delay_task(self, task_name, task_id=None, part_of_set=None,
            task_args=None, task_kwargs=None, **kwargs):
        """INTERNAL"""
        priority = kwargs.get("priority")
        immediate = kwargs.get("immediate")
        mandatory = kwargs.get("mandatory")
        routing_key = kwargs.get("routing_key")
    
        task_args = task_args or []
        task_kwargs = task_kwargs or {}
        task_id = task_id or str(uuid.uuid4())
        message_data = {
            "id": task_id,
            "task": task_name,
            "args": task_args,
            "kwargs": task_kwargs,
        }
        if part_of_set:
            message_data["taskset"] = part_of_set
        self.send(message_data,
                routing_key=routing_key, priority=priority,
                immediate=immediate, mandatory=mandatory)
        return task_id


class TaskConsumerSet(ConsumerSet):
    
    def __init__(self, connection, queues=conf.AMQP_CONSUMER_QUEUES, consumers=[], **options):
        super(TaskConsumerSet, self).__init__(connection, queues=queues, consumers=consumers, **options)

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

    def receive(self, message_data, message):
        pass