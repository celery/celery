from carrot.connection import DjangoAMQPConnection
from celery.conf import AMQP_CONNECTION_TIMEOUT
from celery.messaging import TaskPublisher, TaskConsumer
from celery.log import setup_logger
from celery.result import TaskSetResult
from celery.execute import apply_async, delay_task
from celery.utils import gen_unique_id
from datetime import timedelta

try:
    import cPickle as pickle
except ImportError:
    import pickle


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

    .. attribute:: routing_key

        Override the global default ``routing_key`` for this task.

    .. attribute:: mandatory

        If set, the message has mandatory routing. By default the message
        is silently dropped by the broker if it can't be routed to a queue.
        However - If the message is mandatory, an exception will be raised
        instead.

    .. attribute:: immediate:

        Request immediate delivery. If the message cannot be routed to a
        task worker immediately, an exception will be raised. This is
        instead of the default behaviour, where the broker will accept and
        queue the message, but with no guarantee that the message will ever
        be consumed.

    .. attribute:: priority:

        The message priority. A number from ``0`` to ``9``.

    .. attribute:: ignore_result

        Don't store the status and return value. This means you can't
        use the :class:`celery.result.AsyncResult` to check if the task is
        done, or get its return value. Only use if you need the performance
        and is able live without these features. Any exceptions raised will
        store the return value/status as usual.

    .. attribute:: disable_error_emails

        Disable all error e-mails for this task (only applicable if
        ``settings.SEND_CELERY_ERROR_EMAILS`` is on.)

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
    routing_key = None
    immediate = False
    mandatory = False
    priority = None
    ignore_result = False
    disable_error_emails = False

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
        with this object, i.e.:

            >>> publisher = self.get_publisher()
            >>> # do something with publisher
            >>> publisher.connection.close()

        """
        return TaskPublisher(connection=DjangoAMQPConnection(
                                connect_timeout=AMQP_CONNECTION_TIMEOUT))

    def get_consumer(self):
        """Get a celery task message consumer.

        :rtype: :class:`celery.messaging.TaskConsumer`.

        Please be sure to close the AMQP connection when you're done
        with this object. i.e.:

            >>> consumer = self.get_consumer()
            >>> # do something with consumer
            >>> consumer.connection.close()

        """
        return TaskConsumer(connection=DjangoAMQPConnection(
                                connect_timeout=AMQP_CONNECTION_TIMEOUT))

    @classmethod
    def delay(cls, *args, **kwargs):
        """Delay this task for execution by the ``celery`` daemon(s).

        :param \*args: positional arguments passed on to the task.

        :param \*\*kwargs: keyword arguments passed on to the task.

        :rtype: :class:`celery.result.AsyncResult`

        See :func:`delay_task`.

        """
        return apply_async(cls, args, kwargs)

    @classmethod
    def apply_async(cls, args=None, kwargs=None, **options):
        """Delay this task for execution by the ``celery`` daemon(s).

        :param args: positional arguments passed on to the task.

        :param kwargs: keyword arguments passed on to the task.

        :rtype: :class:`celery.result.AsyncResult`

        See :func:`apply_async`.

        """
        return apply_async(cls, args, kwargs, **options)


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

        >>> taskset_result = taskset.run()
        >>> list_of_return_values = taskset.join()


    """

    def __init__(self, task, args):
        try:
            task_name = task.name
            task_obj = task
        except AttributeError:
            task_name = task
            task_obj = tasks[task_name]

        self.task = task_obj
        self.task_name = task_name
        self.arguments = args
        self.total = len(args)

    def run(self, connect_timeout=AMQP_CONNECTION_TIMEOUT):
        """Run all tasks in the taskset.

        :returns: A :class:`celery.result.TaskSetResult` instance.

        Example

            >>> ts = TaskSet(RefreshFeedTask, [
            ...         ["http://foo.com/rss", {}],
            ...         ["http://bar.com/rss", {}],
            ... )
            >>> result = ts.run()
            >>> result.taskset_id
            "d2c9b261-8eff-4bfb-8459-1e1b72063514"
            >>> result.subtask_ids
            ["b4996460-d959-49c8-aeb9-39c530dcde25",
            "598d2d18-ab86-45ca-8b4f-0779f5d6a3cb"]
            >>> result.waiting()
            True
            >>> time.sleep(10)
            >>> result.ready()
            True
            >>> result.successful()
            True
            >>> result.failed()
            False
            >>> result.join()
            [True, True]

        """
        taskset_id = gen_unique_id()
        conn = DjangoAMQPConnection(connect_timeout=connect_timeout)
        publisher = TaskPublisher(connection=conn)
        subtasks = [apply_async(self.task, args, kwargs,
                                taskset_id=taskset_id, publisher=publisher)
                        for args, kwargs in self.arguments]
        publisher.close()
        conn.close()
        return TaskSetResult(taskset_id, subtasks)

    def iterate(self):
        """Iterate over the results returned after calling :meth:`run`.

        If any of the tasks raises an exception, the exception will
        be re-raised.

        """
        return iter(self.run())

    def join(self, timeout=None):
        """Gather the results for all of the tasks in the taskset,
        and return a list with them ordered by the order of which they
        were called.

        :keyword timeout: The time in seconds, how long
            it will wait for results, before the operation times out.

        :raises TimeoutError: if ``timeout`` is not ``None``
            and the operation takes longer than ``timeout`` seconds.

        If any of the tasks raises an exception, the exception
        will be reraised by :meth:`join`.

        :returns: list of return values for all tasks in the taskset.

        """
        return self.run().join(timeout=timeout)

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
