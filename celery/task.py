from carrot.connection import DjangoAMQPConnection
from celery.log import setup_logger
from celery.conf import TASK_META_USE_DB
from celery.registry import tasks
from celery.messaging import TaskPublisher, TaskConsumer
from celery.models import TaskMeta
from django.core.cache import cache
from datetime import timedelta
from celery.backends import default_backend
import uuid
import traceback


class BasePendingResult(object):
    """Base class for pending result, takes ``backend`` argument."""
    def __init__(self, task_id, backend):
        self.task_id = task_id
        self.backend = backend

    def __str__(self):
        return self.task_id

    def __repr__(self):
        return "<Job: %s>" % self.task_id

    def is_done(self):
        return self.backend.is_done(self.task_id)

    def wait_for(self):
        return self.backend.wait_for(self.task_id)

    @property
    def result(self):
        if self.status == "DONE":
            return self.backend.get_result(self.task_id)
        return None

    @property
    def status(self):
        return self.backend.get_status(self.task_id)


class PendingResult(BasePendingResult):
    """Pending task result using the default backend.""" 

    def __init__(self, task_id):
        super(PendingResult, self).__init__(task_id, backend=default_backend)


def delay_task(task_name, *args, **kwargs):
    """Delay a task for execution by the ``celery`` daemon.

    Examples
    --------
        >>> delay_task("update_record", name="George Constanza", age=32)

    """
    if task_name not in tasks:
        raise tasks.NotRegistered(
                "Task with name %s not registered in the task registry." % (
                    task_name))
    publisher = TaskPublisher(connection=DjangoAMQPConnection())
    task_id = publisher.delay_task(task_name, *args, **kwargs)
    publisher.close()
    return PendingResult(task_id)


def discard_all():
    """Discard all waiting tasks.

    This will ignore all tasks waiting for execution, and they will
    be deleted from the messaging server.

    Returns the number of tasks discarded.

    """
    consumer = TaskConsumer(connection=DjangoAMQPConnection())
    discarded_count = consumer.discard_all()
    consumer.close()
    return discarded_count


def mark_as_done(task_id, result):
    """Mark task as done (executed)."""
    return default_backend.mark_as_done(task_id, result)


def is_done(task_id):
    """Returns ``True`` if task with ``task_id`` has been executed."""
    return default_backend.is_done(task_id)


class Task(object):
    """A task that can be delayed for execution by the ``celery`` daemon.

    All subclasses of ``Task`` has to define the ``name`` attribute, which is
    the name of the task that can be passed to ``celery.task.delay_task``,
    it also has to define the ``run`` method, which is the actual method the
    ``celery`` daemon executes.
    

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
    max_retries = 0 # unlimited
    retry_interval = timedelta(seconds=2)
    auto_retry = False

    def __init__(self):
        if not self.name:
            raise NotImplementedError("Tasks must define a name attribute.")

    def __call__(self, *args, **kwargs):
        """The ``__call__`` is called when you do ``Task().run()`` and calls
        the ``run`` method. It also catches any exceptions and logs them."""
        try:
            retval = self.run(*args, **kwargs)
        except Exception, e:
            logger = self.get_logger(**kwargs)
            logger.critical("Task got exception %s: %s\n%s" % (
                                e.__class__, e, traceback.format_exc()))
            self.handle_exception(e, args, kwargs)
            if self.auto_retry:
                self.retry(kwargs["task_id"], args, kwargs)
            return
        else:
            return retval

    def run(self, *args, **kwargs):
        """The actual task. All subclasses of :class:`Task` must define
        the run method, if not a ``NotImplementedError`` exception is raised.
        """
        raise NotImplementedError("Tasks must define a run method.")

    def get_logger(self, **kwargs):
        """Get a process-aware logger object."""
        return setup_logger(**kwargs)

    def get_publisher(self):
        """Get a celery task message publisher."""
        return TaskPublisher(connection=DjangoAMQPConnection())

    def get_consumer(self):
        """Get a celery task message consumer."""
        return TaskConsumer(connection=DjangoAMQPConnection())

    def requeue(self, task_id, args, kwargs):
        self.get_publisher().requeue_task(self.name, task_id, args, kwargs)

    def retry(self, task_id, args, kwargs):
        retry_queue.put(self.name, task_id, args, kwargs)

    def handle_exception(self, exception, retry_args, retry_kwargs):
        pass

    @classmethod
    def delay(cls, *args, **kwargs):
        """Delay this task for execution by the ``celery`` daemon(s)."""
        return delay_task(cls.name, *args, **kwargs)


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
        publisher = TaskPublisher(connection=DjangoAMQPConnection())
        subtask_ids = []
        for arg in self.arguments:
            subtask_id = publisher.delay_task_in_set(task_name=self.task_name,
                                                     taskset_id=taskset_id,
                                                     task_args=[],
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


class DeleteExpiredTaskMetaTask(PeriodicTask):
    """A periodic task that deletes expired task metadata every day.
   
    It's only registered if ``settings.CELERY_TASK_META_USE_DB`` is set.
    """
    name = "celery.delete_expired_task_meta"
    run_every = timedelta(days=1)

    def run(self, **kwargs):
        logger = self.get_logger(**kwargs)
        logger.info("Deleting expired task meta objects...")
        default_backend.cleanup()
tasks.register(DeleteExpiredTaskMetaTask)
