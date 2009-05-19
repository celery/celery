"""

Working with tasks and task sets.

"""
from carrot.connection import DjangoAMQPConnection
from celery.log import setup_logger
from celery.registry import tasks
from celery.messaging import TaskPublisher, TaskConsumer
from celery.models import TaskMeta
from django.core.cache import cache
from datetime import timedelta
from celery.backends import default_backend
from celery.datastructures import PositionQueue
from celery.result import AsyncResult
from celery.timer import TimeoutTimer
import uuid
import pickle


def delay_task(task_name, *args, **kwargs):
    """Delay a task for execution by the ``celery`` daemon.

    :param task_name: the name of a task registered in the task registry.

    :param \*args: positional arguments to pass on to the task.
    
    :param \*\*kwargs: keyword arguments to pass on to the task.
    
    :raises celery.registry.NotRegistered: exception if no such task 
        has been registered in the task registry.

    :rtype: :class:`celery.result.AsyncResult`. 

    Example

        >>> r = delay_task("update_record", name="George Constanza", age=32)
        >>> r.ready()
        True
        >>> r.result
        "Record was updated"

    """
    if task_name not in tasks:
        raise tasks.NotRegistered(
                "Task with name %s not registered in the task registry." % (
                    task_name))
    amqp_connection = DjangoAMQPConnection()
    publisher = TaskPublisher(connection=amqp_connection)
    task_id = publisher.delay_task(task_name, *args, **kwargs)
    amqp_connection.close()
    return AsyncResult(task_id)


def discard_all():
    """Discard all waiting tasks.

    This will ignore all tasks waiting for execution, and they will
    be deleted from the messaging server.

    :returns: the number of tasks discarded.

    :rtype: int

    """
    amqp_connection = DjangoAMQPConnection()
    consumer = TaskConsumer(connection=amqp_connection)
    discarded_count = consumer.discard_all()
    amqp_connection.close()
    return discarded_count


def mark_as_done(task_id, result):
    """Mark task as done (executed successfully).
   
    :param task_id: id of the task.
    
    :param result: the return value of the task.

    """
    default_backend.mark_as_done(task_id, result)


def mark_as_failure(task_id, exc):
    """Mark task as done (executed).
    
    :param task_id: id of the task.

    :param exc: the exception instance raised by the task.

    """
    default_backend.mark_as_failure(task_id, exc)


def is_done(task_id):
    """Returns ``True`` if task with ``task_id`` has been executed.
   
    :rtype: bool

    """
    return default_backend.is_done(task_id)


class Task(object):
    """A task that can be delayed for execution by the ``celery`` daemon.

    All subclasses of :class:`Task` must define the :meth:`run` method,
    which is the actual method the ``celery`` daemon executes.

    The :meth:`run` method supports both positional, and keyword arguments.
    
    .. attribute:: name

        *REQUIRED* All subclasses of :class:`Task` has to define the
        :attr:`name` attribute. This is the name of the task, registered
        in the task registry, and passed to :func:`delay_task`.
        
    .. attribute:: type

        The type of task, currently this can be ``regular``, or ``periodic``,
        however if you want a periodic task, you should subclass
        :class:`PeriodicTask` instead.
        
    :raises NotImplementedError: if the :attr:`name` attribute is not set.

    The resulting class is callable, which if called will apply the
    :meth:`run` method.

    Examples
    
    This is a simple task just logging a message,

        >>> from celery.task import tasks, Task
        >>> class MyTask(Task):
        ...     name = "mytask"
        ...
        ...     def run(self, some_arg=None, **kwargs):
        ...         logger = self.get_logger(**kwargs)
        ...         logger.info("Running MyTask with arg some_arg=%s" %
        ...                     some_arg))
        ...         return 42
        ... tasks.register(MyTask)

    You can delay the task using the classmethod :meth:`delay`...

        >>> result = MyTask.delay(some_arg="foo")
        >>> result.status # after some time
        'DONE'
        >>> result.result
        42

    ...or using the :func:`delay_task` function, by passing the name of
    the task.

        >>> from celery.task import delay_task
        >>> result = delay_task(MyTask.name, some_arg="foo")


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
        return self.run(*args, **kwargs)

    def run(self, *args, **kwargs):
        """*REQUIRED* The actual task.
        
        All subclasses of :class:`Task` must define the run method.
        
        :raises NotImplementedError: by default, so you have to override
            this method in your subclass.

        """
        raise NotImplementedError("Tasks must define a run method.")

    def get_logger(self, **kwargs):
        """Get process-aware logger object.

        See :func:`celery.log.setup_logger`.
        
        """
        return setup_logger(**kwargs)

    def get_publisher(self):
        """Get a celery task message publisher.
        
        :rtype: :class:`celery.messaging.TaskPublisher`.

        Please be sure to close the AMQP connection when you're done
        with this object, i.e:

            >>> publisher = self.get_publisher()
            >>> # do something with publisher
            >>> publisher.connection.close()

        """
        return TaskPublisher(connection=DjangoAMQPConnection())

    def get_consumer(self):
        """Get a celery task message consumer.
       
        :rtype: :class:`celery.messaging.TaskConsumer`.

        Please be sure to close the AMQP connection when you're done
        with this object. i.e:

            >>> consumer = self.get_consumer()
            >>> # do something with consumer
            >>> consumer.connection.close()

        """
        return TaskConsumer(connection=DjangoAMQPConnection())

    def requeue(self, task_id, args, kwargs):
        publisher = self.get_publisher()
        publisher.requeue_task(self.name, task_id, args, kwargs)
        publisher.connection.close()

    def retry(self, task_id, args, kwargs):
        retry_queue.put(self.name, task_id, args, kwargs)

    @classmethod
    def delay(cls, *args, **kwargs):
        """Delay this task for execution by the ``celery`` daemon(s).

        :param \*args: positional arguments passed on to the task.

        :param \*\*kwargs: keyword arguments passed on to the task.
        
        :rtype: :class:`celery.result.AsyncResult`

        See :func:`delay_task`.

        """
        return delay_task(cls.name, *args, **kwargs)


class TaskSet(object):
    """A task containing several subtasks, making it possible
    to track how many, or when all of the tasks has been completed.

    :param task: The task class or name.
        Can either be a fully qualified task name, or a task class.

    :param args: A list of args, kwargs pairs.
        e.g. ``[[args1, kwargs1], [args2, kwargs2], ..., [argsN, kwargsN]]``


    .. attribute:: task_name

        The name of the task.

    .. attribute:: arguments

        The arguments, as passed to the task set constructor.

    .. attribute:: total

        Total number of tasks in this task set.
   
    Example

        >>> from djangofeeds.tasks import RefreshFeedTask
        >>> taskset = TaskSet(RefreshFeedTask, args=[
        ...                 [], {"feed_url": "http://cnn.com/rss"},
        ...                 [], {"feed_url": "http://bbc.com/rss"},
        ...                 [], {"feed_url": "http://xkcd.com/rss"}])

        >>> taskset_id, subtask_ids = taskset.run()
        >>> list_of_return_values = taskset.join()
        

    """

    def __init__(self, task, args):
        try:
            task_name = task.name
        except AttributeError:
            task_name = task

        self.task_name = task_name
        self.arguments = args
        self.total = len(args)

    def run(self):
        """Run all tasks in the taskset.

        :returns: A tuple containing the taskset id, and a list
            of subtask ids.

        :rtype: tuple

        Example

            >>> ts = RefreshFeeds([
            ...         ["http://foo.com/rss", {}],
            ...         ["http://bar.com/rss", {}],
            ... )
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
        amqp_connection = DjangoAMQPConnection()
        publisher = TaskPublisher(connection=amqp_connection)
        subtask_ids = []
        for arg, kwarg in self.arguments:
            subtask_id = publisher.delay_task_in_set(task_name=self.task_name,
                                                     taskset_id=taskset_id,
                                                     task_args=arg,
                                                     task_kwargs=kwarg)
            subtask_ids.append(subtask_id) 
        amqp_connection.close()
        return taskset_id, subtask_ids

    def iterate(self):
        """Iterate over the results returned after calling :meth:`run`.
        
        If any of the tasks raises an exception, the exception will
        be re-raised.

        """
        taskset_id, subtask_ids = self.run()
        results = dict([(task_id, AsyncResult(task_id))
                            for task_id in subtask_ids])
        while results:
            for task_id, pending_result in results.items():
                if pending_result.status == "DONE":
                    del(results[task_id])
                    yield pending_result.result
                elif pending_result.status == "FAILURE":
                    raise pending_result.result

    def join(self, timeout=None):
        """Gather the results for all of the tasks in the taskset,
        and return a list with them ordered by the order of which they
        were called.

        :keyword timeout: The time in seconds, how long
            it will wait for results, before the operation times out.

        :raises celery.timer.TimeoutError: if ``timeout`` is not ``None``
            and the operation takes longer than ``timeout`` seconds.

        If any of the tasks raises an exception, the exception
        will be reraised by :meth:`join`.

        :returns: list of return values for all tasks in the taskset.

        """
        timeout_timer = TimeoutTimer(timeout) # Timeout timer starts here.
        taskset_id, subtask_ids = self.run()
        pending_results = map(AsyncResult, subtask_ids)
        results = PositionQueue(length=len(subtask_ids))

        while True:
            for position, pending_result in enumerate(pending_results):
                if pending_result.status == "DONE":
                    results[position] = pending_result.result
                elif pending_result.status == "FAILURE":
                    raise pending_result.result
            if results.full():
                # Make list copy, so the returned type is not a position
                # queue.
                return list(results)

            # This raises TimeoutError when timed out.
            timeout_timer.tick()

    @classmethod
    def remote_execute(cls, func, args):
        """Apply ``args`` to function by distributing the args to the
        celery server(s)."""
        pickled = pickle.dumps(func)
        arguments = [[[pickled, arg, {}], {}] for arg in args]
        return cls(ExecuteRemoteTask, arguments)

    @classmethod
    def map(cls, func, args, timeout=None):
        """Distribute processing of the arguments and collect the results."""
        remote_task = cls.remote_execute(func, args)
        return remote_task.join(timeout=timeout)

    @classmethod
    def map_async(cls, func, args, timeout=None):
        """Distribute processing of the arguments and collect the results
        asynchronously.
        
        :returns: :class:`celery.result.AsyncResult` instance.
        
        """
        serfunc = pickle.dumps(func)
        return AsynchronousMapTask.delay(serfunc, args, timeout=timeout)


def dmap(func, args, timeout=None):
    """Distribute processing of the arguments and collect the results.

    Example

        >>> from celery.task import map
        >>> import operator
        >>> dmap(operator.add, [[2, 2], [4, 4], [8, 8]])
        [4, 8, 16]

    """
    return TaskSet.map(func, args, timeout=timeout)


class AsynchronousMapTask(Task):
    """Task used internally by :func:`dmap_async` and
    :meth:`TaskSet.map_async`.  """
    name = "celery.map_async"

    def run(self, serfunc, args, **kwargs):
        timeout = kwargs.get("timeout")
        return TaskSet.map(pickle.loads(serfunc), args, timeout=timeout)
tasks.register(AsynchronousMapTask)


def dmap_async(func, args, timeout=None):
    """Distribute processing of the arguments and collect the results
    asynchronously.
    
    :returns: :class:`celery.result.AsyncResult` object.

    Example

        >>> from celery.task import dmap_async
        >>> import operator
        >>> presult = dmap_async(operator.add, [[2, 2], [4, 4], [8, 8]])
        >>> presult
        <AsyncResult: 373550e8-b9a0-4666-bc61-ace01fa4f91d>
        >>> presult.status
        'DONE'
        >>> presult.result
        [4, 8, 16]

    """
    return TaskSet.map_async(func, args, timeout=timeout)


class PeriodicTask(Task):
    """A periodic task is a task that behaves like a :manpage:`cron` job.

    .. attribute:: run_every
    
        *REQUIRED* Defines how often the task is run (its interval),
        it can be either a :class:`datetime.timedelta` object or an
        integer specifying the time in seconds.

    :raises NotImplementedError: if the :attr:`run_every` attribute is
        not defined.

    You have to register the periodic task in the task registry.

    Example

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


class ExecuteRemoteTask(Task):
    """Execute an arbitrary function or object.

    *Note* You probably want :func:`execute_remote` instead, which this
    is an internal component of.

    The object must be pickleable, so you can't use lambdas or functions
    defined in the REPL (that is the python shell, or ``ipython``).
    
    """
    name = "celery.execute_remote"

    def run(self, ser_callable, fargs, fkwargs, **kwargs):
        """
        :param ser_callable: A pickled function or callable object. 

        :param fargs: Positional arguments to apply to the function.

        :param fkwargs: Keyword arguments to apply to the function.

        """
        callable_ = pickle.loads(ser_callable)
        return callable_(*fargs, **fkwargs)
tasks.register(ExecuteRemoteTask)


def execute_remote(func, *args, **kwargs):
    """Execute arbitrary function/object remotely.
        
    :param func: A callable function or object.

    :param \*args: Positional arguments to apply to the function.

    :param \*\*kwargs: Keyword arguments to apply to the function.

    The object must be picklable, so you can't use lambdas or functions
    defined in the REPL (the objects must have an associated module).

    :returns: class:`celery.result.AsyncResult`.
    
    """
    return ExecuteRemoteTask.delay(pickle.dumps(func), args, kwargs)


class DeleteExpiredTaskMetaTask(PeriodicTask):
    """A periodic task that deletes expired task metadata every day.

    This runs the current backend's
    :meth:`celery.backends.base.BaseBackend.cleanup` method.

    """
    name = "celery.delete_expired_task_meta"
    run_every = timedelta(days=1)

    def run(self, **kwargs):
        logger = self.get_logger(**kwargs)
        logger.info("Deleting expired task meta objects...")
        default_backend.cleanup()
tasks.register(DeleteExpiredTaskMetaTask)
