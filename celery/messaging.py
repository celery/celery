"""celery.messaging"""
from carrot.messaging import Publisher, Consumer
from celery import conf
import uuid


class NoProcessConsumer(Consumer):
    """A consumer that raises an error if used with wait callbacks (i.e.
    it doesn't support ``carrot.messaging.Consumer.wait``)."""
    
    def receive(self, message_data, message):
        raise NotImplementedError(
                "Don't use process_next() or wait() with the TaskConsumer!")


class TaskPublisher(Publisher):
    """The AMQP Task Publisher class."""
    exchange = conf.AMQP_EXCHANGE
    routing_key = conf.AMQP_ROUTING_KEY

    def delay_task(self, task_name, *task_args, **task_kwargs):
        """Delay task for execution by the celery nodes."""
        return self._delay_task(task_name=task_name, args=task_args,
                                kwargs=task_kwargs)


    def delay_task_in_set(self, task_name, taskset_id, task_args,
            task_kwargs):
        """Delay a task which part of a task set."""
        return self._delay_task(task_name=task_name, part_of_set=taskset_id,
                                args=task_args, kwargs=task_kwargs)
    
    def requeue_task(self, task_name, task_id, task_args, task_kwargs,
            part_of_set=None):
        """Requeue a failed task."""
        return self._delay_task(task_name=task_name, part_of_set=part_of_set,
                                task_id=task_id, args=task_args,
                                kwargs=task_kwargs)

    def _delay_task(self, task_name, task_id=None, part_of_set=None,
            args=None, kwargs=None):
        args = args or []
        kwargs = kwargs or {}
        task_id = task_id or str(uuid.uuid4())
        message_data = {
            "id": task_id,
            "task": task_name,
            "args": args,
            "kwargs": kwargs,
        }
        if part_of_set:
            message_data["taskset"] = part_of_set
        self.send(message_data)
        return task_id


class TaskConsumer(NoProcessConsumer):
    """The AMQP Task Consumer class."""
    queue = conf.AMQP_CONSUMER_QUEUE
    exchange = conf.AMQP_EXCHANGE
    routing_key = conf.AMQP_ROUTING_KEY
