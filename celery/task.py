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


def delay_task(task_name, **kwargs):
    """Delay a task for execution by the ``celery`` daemon.

    Examples
    --------
        >>> delay_task("update_record", name="George Constanza", age=32)

    """
    if task_name not in tasks:
        raise tasks.NotRegistered(
                "Task with name %s not registered in the task registry." % (
                    task_name))
    publisher = TaskPublisher(connection=DjangoAMQPConnection)
    task_id = publisher.delay_task(task_name, **kwargs)
    publisher.close()
    return task_id


def discard_all():
    """Discard all waiting tasks.

    This will ignore all tasks waiting for execution, and they will
    be deleted from the messaging server.

    Returns the number of tasks discarded.

    """
    consumer = TaskConsumer(connection=DjangoAMQPConnection)
    discarded_count = consumer.discard_all()
    consumer.close()
    return discarded_count


def gen_task_done_cache_key(task_id):
    """Generate a cache key for marking a task as done."""
    return "celery-task-done-marker-%s" % task_id


def mark_as_done(task_id, result):
    """Mark task as done (executed).

    if ``settings.TASK_META_USE_DB`` is ``True``, this will
    use the :class:`celery.models.TaskMeta` model, if not memcached
    is used.

    """
    if result is None:
        result = True
    if TASK_META_USE_DB:
        TaskMeta.objects.mark_as_done(task_id)
    else:
        cache_key = gen_task_done_cache_key(task_id)
        cache.set(cache_key, result)


def is_done(task_id):
    """Returns ``True`` if task with ``task_id`` has been executed."""
    if TASK_META_USE_DB:
        return TaskMeta.objects.is_done(task_id)
    else:
        cache_key = gen_task_done_cache_key(task_id)
        return bool(cache.get(cache_key))


class Task(object):
    """A task that can be delayed for execution by the ``celery`` daemon.

    All subclasses of ``Task`` has to define the ``name`` attribute, which is
    the name of the task that can be passed to ``celery.task.delay_task``,
    it also has to define the ``run`` method, which is the actual method the
    ``celery`` daemon executes. This method does not support positional
    arguments, only keyword arguments.

    Examples
    --------

    This is a simple task just logging a message,

        >>> from celery.task import tasks, Task
        >>> class MyTask(Task):
        ...     name = "mytask"
        ...
        ...     def run(self, some_arg=None, **kwargs):
        ...         logger = self.get_logger(**kwargs)
        ...         logger.info("Running MyTask with arg some_arg=%s" %
        ...                     some_arg))
        ... tasks.register(MyTask)

    You can delay the task using the classmethod ``delay``...

        >>> MyTask.delay(some_arg="foo")

    ...or using the ``celery.task.delay_task`` function, by passing the
    name of the task.

        >>> from celery.task import delay_task
        >>> delay_task(MyTask.name, some_arg="foo")

    """
    name = None
    type = "regular"

    def __init__(self):
        if not self.name:
            raise NotImplementedError("Tasks must define a name attribute.")

    def __call__(self, **kwargs):
        """The ``__call__`` is called when you do ``Task().run()`` and calls
        the ``run`` method. It also catches any exceptions and logs them."""
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
        """The actual task. All subclasses of :class:`Task` must define
        the run method, if not a ``NotImplementedError`` exception is raised.
        """
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
        """Delay this task for execution by the ``celery`` daemon(s)."""
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

        >>> taskset_id, subtask_ids = taskset.run()
        

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
        """Run all tasks in the taskset.

        Returns a tuple with the taskset id, and a list of subtask id's.

        Examples
        --------
            >>> ts = RefreshFeeds(["http://foo.com/rss", http://bar.com/rss"])
            >>> taskset_id, subtask_ids = ts.run()
            >>> taskset_id
            "d2c9b261-8eff-4bfb-8459-1e1b72063514"
            >>> subtask_ids
            ["b4996460-d959-49c8-aeb9-39c530dcde25",
            "598d2d18-ab86-45ca-8b4f-0779f5d6a3cb"]
            >>> time.sleep(10)
            >>> is_done(taskset_id)
            True
        """
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
    """A periodic task is a task that behaves like a cron job.

    The ``run_every`` attribute defines how often the task is run (its
    interval), it can be either a ``datetime.timedelta`` object or a integer
    specifying the time in seconds.

    You have to register the periodic task in the task registry.

    Examples
    --------

        >>> from celery.task import tasks, PeriodicTask
        >>> from datetime import timedelta
        >>> class MyPeriodicTask(PeriodicTask):
        ...     name = "my_periodic_task"
        ...     run_every = timedelta(seconds=30)
        ...
        ...     def run(self, **kwargs):
        ...         logger = self.get_logger(**kwargs)
        ...         logger.info("Running MyPeriodicTask")
        >>> tasks.register(MyPeriodicTask)

    """
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
    """A simple test task that just logs something."""
    name = "celery.test_task"

    def run(self, some_arg, **kwargs):
        logger = self.get_logger(**kwargs)
        logger.info("TestTask got some_arg=%s" % some_arg)
tasks.register(TestTask)


class DeleteExpiredTaskMetaTask(PeriodicTask):
    """A periodic task that deletes expired task metadata every day.
   
    It's only registered if ``settings.CELERY_TASK_META_USE_DB`` is set.
    """
    name = "celery.delete_expired_task_meta"
    run_every = timedelta(days=1)

    def run(self, **kwargs):
        logger = self.get_logger(**kwargs)
        logger.info("Deleting expired task meta objects...")
        TaskMeta.objects.delete_expired()
if TASK_META_USE_DB:
    tasks.register(DeleteExpiredTaskMetaTask)
