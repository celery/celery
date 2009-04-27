from carrot.messaging import Publisher, Consumer
import uuid


class NoProcessConsumer(Consumer):
    
    def receive(self, message_data, message):
        raise NotImplementedError(
                "Don't use process_next() or wait() with the TaskConsumer!")


class TaskPublisher(Publisher):
    exchange = "celery"
    routing_key = "celery"

    def delay_task(self, task_name, **kwargs):
        task_id = uuid.uuid4()
        message_data = dict(kwargs)
        message_data["celeryTASK"] = task_name
        message_data["celeryID"] = str(task_id)
        self.send(message_data)
        return task_id


class TaskConsumer(NoProcessConsumer):
    queue = "celery"
    exchange = "celery"
    routing_key = "celery"
