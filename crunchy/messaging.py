from carrot.messaging import Publisher, Consumer
import uuid


class TaskPublisher(Publisher):
    queue = "crunchy"
    exchange = "crunchy"
    routing_key = "crunchy"

    def delay_task(self, task_name, **kwargs):
        task_id = uuid.uuid4()
        message_data = dict(kwargs)
        message_data["crunchTASK"] = task_name
        message_data["crunchID"] = str(task_id)
        self.send(message_data)
        return task_id


class TaskConsumer(Consumer):
    queue = "crunchy"
    exchange = "crunchy"
    routing_key = "crunchy"

    def receive(self, message_data, message):
        raise NotImplementedError(
                "Don't use process_next() or wait() with the TaskConsumer!")
