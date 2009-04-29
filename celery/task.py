from carrot.connection import DjangoAMQPConnection
from celery.log import setup_logger
from celery.conf import TASK_META_USE_DB
from celery.registry import tasks
from celery.messaging import TaskPublisher, TaskConsumer
from celery.models import TaskMeta
from django.core.cache import cache
from datetime import timedelta
import uuid
import traceback

__all__ = ["delay_task", "discard_all", "gen_task_done_cache_key",
           "mark_as_done", "is_done", "Task", "PeriodicTask", "TestTask"]


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


def gen_task_done_cache_key(task_id):
    return "celery-task-done-marker-%s" % task_id


def mark_as_done(task_id, result):
    if result is None:
        result = True
    if TASK_META_USE_DB:
        TaskMeta.objects.mark_as_done(task_id)
    else:
        cache_key = gen_task_done_cache_key(task_id)
        cache.set(cache_key, result)


def is_done(task_id):
    if TASK_META_USE_DB:
        return TaskMeta.objects.is_done(task_id)
    else:
        cache_key = gen_task_done_cache_key(task_id)
        return cache.get(cache_key)


class Task(object):
    name = None
    type = "regular"

    def __init__(self):
        if not self.name:
            raise NotImplementedError("Tasks must define a name attribute.")

    def __call__(self, **kwargs):
        try:
            retval = self.run(**kwargs)
        except Exception, e:
            logger = self.get_logger(**kwargs)
            logger.critical("Task got exception %s: %s\n%s" % (
                                e.__class__, e, traceback.format_exc()))
            return
        else:
            return retval

    def run(self, **kwargs):
        raise NotImplementedError("Tasks must define a run method.")

    def get_logger(self, **kwargs):
        """Get a process-aware logger object."""
        return setup_logger(**kwargs)

    def get_publisher(self):
        """Get a celery task message publisher."""
        return TaskPublisher(connection=DjangoAMQPConnection)

    def get_consumer(self):
        """Get a celery task message consumer."""
        return TaskConsumer(connection=DjangoAMQPConnection)

    @classmethod
    def delay(cls, **kwargs):
        return delay_task(cls.name, **kwargs)


class TaskSet(object):
    """A task containing several subtasks, making it possible
    to track how many, or when all of the tasks are completed.
    
    Example Usage
    --------------

        >>> from djangofeeds.tasks import RefreshFeedTask
        >>> taskset = TaskSet(RefreshFeedTask, args=[
        ...                 {"feed_url": "http://cnn.com/rss"},
        ...                 {"feed_url": "http://bbc.com/rss"},
        ...                 {"feed_url": "http://xkcd.com/rss"}])

        >>> taskset_id = taskset.delay()
        

    """

    def __init__(self, task, args):
        """``task`` can be either a fully qualified task name, or a task
        class, args is a list of arguments for the subtasks.
        """

        try:
            task_name = task.name
        except AttributeError:
            task_name = task

        self.task_name = task_name
        self.arguments = args
        self.total = len(args)

    def run(self):
        taskset_id = str(uuid.uuid4())
        publisher = TaskPublisher(connection=DjangoAMQPConnection)
        subtask_ids = []
        for arg in self.arguments:
            subtask_id = publisher.delay_task_in_set(task_name=self.task_name,
                                                     taskset_id=taskset_id,
                                                     task_kwargs=arg)
            subtask_ids.append(subtask_id) 
        publisher.close()
        return taskset_id, subtask_ids


class PeriodicTask(Task):
    run_every = timedelta(days=1)
    type = "periodic"

    def __init__(self):
        if not self.run_every:
            raise NotImplementedError(
                    "Periodic tasks must have a run_every attribute")

        # If run_every is a integer, convert it to timedelta seconds.
        if isinstance(self.run_every, int):
            self.run_every = timedelta(seconds=self.run_every)

        super(PeriodicTask, self).__init__()


class TestTask(Task):
    name = "celery.test_task"

    def run(self, some_arg, **kwargs):
        logger = self.get_logger(**kwargs)
        logger.info("TestTask got some_arg=%s" % some_arg)
tasks.register(TestTask)


class DeleteExpiredTaskMetaTask(PeriodicTask):
    name = "celery.delete_expired_task_meta"
    run_every = timedelta(days=1)

    def run(self, **kwargs):
        logger = self.get_logger(**kwargs)
        logger.info("Deleting expired task meta objects...")
        TaskMeta.objects.delete_expired()
if TASK_META_USE_DB:
    tasks.register(DeleteExpiredTaskMetaTask)
