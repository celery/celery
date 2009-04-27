from carrot.connection import DjangoAMQPConnection
from crunchy.log import setup_logger
from crunchy.registry import tasks
from crunchy.messaging import TaskPublisher, TaskConsumer


def delay_task(task_name, **kwargs):
    if task_name not in tasks:
        raise tasks.NotRegistered(
                "Task with name %s not registered in the task registry." % (
                    task_name))
    publisher = TaskPublisher(connection=DjangoAMQPConnection)
    task_id = publisher.delay_task(task_name, **kwargs)
    publisher.close()
    return task_id


def discard_all():
    consumer = TaskConsumer(connection=DjangoAMQPConnection)
    discarded_count = consumer.discard_all()
    consumer.close()
    return discarded_count


class Task(object):
    name = None
    type = "regular"

    def __init__(self):
        if not self.name:
            raise NotImplementedError("Tasks must define a name attribute.")

    def __call__(self, **kwargs):
        return self.run(**kwargs)

    def run(self, **kwargs):
        raise NotImplementedError("Tasks must define a run method.")

    def get_logger(self, **kwargs):
        """Get a process-aware logger object."""
        return setup_logger(**kwargs)

    def get_publisher(self):
        """Get a crunchy task message publisher."""
        return TaskPublisher(connection=DjangoAMQPConnection)

    def get_consumer(self):
        """Get a crunchy task message consumer."""
        return TaskConsumer(connection=DjangoAMQPConnection)

    @classmethod
    def delay(cls, **kwargs):
        return delay_task(cls.name, **kwargs)


class PeriodicTask(Task):
    run_every = 86400
    type = "periodic"

    def __init__(self):
        if not self.run_every:
            raise NotImplementedError(
                    "Periodic tasks must have a run_every attribute")
        super(PeriodicTask, self).__init__()


class TestTask(Task):
    name = "crunchy-test-task"

    def run(self, some_arg, **kwargs):
        logger = self.get_logger(**kwargs)
        logger.info("TestTask got some_arg=%s" % some_arg)

    def after(self, task_id):
        logger = self.get_logger(**kwargs)
        logger.info("TestTask with id %s was successfully executed." % task_id)
tasks.register(TestTask)
