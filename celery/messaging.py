from carrot.messaging import Publisher, Consumer
import uuid

__all__ = ["NoProcessConsumer", "TaskPublisher", "TaskConsumer"]


class NoProcessConsumer(Consumer):
    
    def receive(self, message_data, message):
        raise NotImplementedError(
                "Don't use process_next() or wait() with the TaskConsumer!")


class TaskPublisher(Publisher):
    exchange = "celery"
    routing_key = "celery"

    def delay_task(self, task_name, **task_kwargs):
        return self._delay_task(task_name=task_name, extra_data=task_kwargs)

    def delay_task_in_set(self, task_name, taskset_id, task_kwargs):
        return self._delay_task(task_name=task_name, part_of_set=taskset_id,
                                extra_data=task_kwargs)

    def _delay_task(self, task_name, part_of_set=None, extra_data=None):
        extra_data = extra_data or {}
        task_id = str(uuid.uuid4())
        message_data = dict(extra_data)
        message_data["celeryTASK"] = task_name
        message_data["celeryID"] = task_id
        if part_of_set:
            message_data["celeryTASKSET"] = part_of_set
        self.send(message_data)


class TaskConsumer(NoProcessConsumer):
    queue = "celery"
    exchange = "celery"
    routing_key = "celery"
