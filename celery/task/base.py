import sys
import warnings
from datetime import datetime, timedelta
from Queue import Queue

from billiard.serialization import pickle

from celery import conf
from celery.log import setup_logger
from celery.utils import gen_unique_id, get_full_cls_name
from celery.result import TaskSetResult, EagerResult
from celery.execute import apply_async, apply
from celery.registry import tasks
from celery.backends import default_backend
from celery.messaging import TaskPublisher, TaskConsumer
from celery.messaging import establish_connection as _establish_connection
from celery.exceptions import MaxRetriesExceededError, RetryTaskError


class TaskType(type):
    """Metaclass for tasks.

    Automatically registers the task in the task registry, except
    if the ``abstract`` attribute is set.

    If no ``name`` attribute is provided, the name is automatically
    set to the name of the module it was defined in, and the class name.

    """

    def __new__(cls, name, bases, attrs):
        super_new = super(TaskType, cls).__new__
        task_module = attrs["__module__"]

        # Abstract class, remove the abstract attribute so
        # any class inheriting from this won't be abstract by default.
        if attrs.pop("abstract", None) or not attrs.get("autoregister", True):
            return super_new(cls, name, bases, attrs)

        # Automatically generate missing name.
        if not attrs.get("name"):
            task_module = sys.modules[task_module]
            task_name = ".".join([task_module.__name__, name])
            attrs["name"] = task_name

        # Because of the way import happens (recursively)
        # we may or may not be the first time the task tries to register
        # with the framework. There should only be one class for each task
        # name, so we always return the registered version.

        task_name = attrs["name"]
        if task_name not in tasks:
            task_cls = super_new(cls, name, bases, attrs)
            tasks.register(task_cls)
        return tasks[task_name].__class__


class Task(object):
    """A celery task.

    All subclasses of :class:`Task` must define the :meth:`run` method,
    which is the actual method the ``celery`` daemon executes.

    The :meth:`run` method can take use of the default keyword arguments,
    as listed in the :meth:`run` documentation.

    .. attribute:: name
        Name of the task.

    .. attribute:: abstract
        If ``True`` the task is an abstract base class.

    .. attribute:: type

        The type of task, currently this can be ``regular``, or ``periodic``,
        however if you want a periodic task, you should subclass
        :class:`PeriodicTask` instead.

    .. attribute:: routing_key

        Override the global default ``routing_key`` for this task.

    .. attribute:: exchange

        Override the global default ``exchange`` for this task.

    .. attribute:: mandatory

        Mandatory message routing. An exception will be raised if the task
        can't be routed to a queue.

    .. attribute:: immediate:

        Request immediate delivery. An exception will be raised if the task
        can't be routed to a worker immediately.

    .. attribute:: priority:
        The message priority. A number from ``0`` to ``9``, where ``0`` is the
        highest. Note that RabbitMQ doesn't support priorities yet.

    .. attribute:: max_retries

        Maximum number of retries before giving up.

    .. attribute:: default_retry_delay

        Default time in seconds before a retry of the task should be
        executed. Default is a 1 minute delay.

    .. attribute:: rate_limit

        Set the rate limit for this task type, Examples: ``None`` (no rate
        limit), ``"100/s"`` (hundred tasks a second), ``"100/m"`` (hundred
        tasks a minute), ``"100/h"`` (hundred tasks an hour)

    .. attribute:: rate_limit_queue_type

        Type of queue used by the rate limiter for this kind of tasks.
        Default is a :class:`Queue.Queue`, but you can change this to
        a :class:`Queue.LifoQueue` or an invention of your own.

    .. attribute:: ignore_result

        Don't store the return value of this task.

    .. attribute:: disable_error_emails

        Disable all error e-mails for this task (only applicable if
        ``settings.SEND_CELERY_ERROR_EMAILS`` is on.)

    .. attribute:: serializer

        The name of a serializer that has been registered with
        :mod:`carrot.serialization.registry`. Example: ``"json"``.

    .. attribute:: backend

        The result store backend used for this task.

    .. attribute:: autoregister
        If ``True`` the task is automatically registered in the task
        registry, which is the default behaviour.


    The resulting class is callable, which if called will apply the
    :meth:`run` method.

    """
    __metaclass__ = TaskType

    name = None
    abstract = True
    autoregister = True
    type = "regular"
    exchange = None
    routing_key = None
    immediate = False
    mandatory = False
    priority = None
    ignore_result = False
    disable_error_emails = False
    max_retries = 3
    default_retry_delay = 3 * 60
    serializer = conf.TASK_SERIALIZER
    rate_limit = conf.DEFAULT_RATE_LIMIT
    rate_limit_queue_type = Queue
    backend = default_backend

    MaxRetriesExceededError = MaxRetriesExceededError

    def __init__(self):
        if not self.__class__.name:
            self.__class__.name = get_full_cls_name(self.__class__)

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def run(self, *args, **kwargs):
        """The body of the task executed by the worker.

        The following standard keyword arguments are reserved and is passed
        by the worker if the function/method supports them:

            * task_id
            * task_name
            * task_retries
            * logfile
            * loglevel

        Additional standard keyword arguments may be added in the future.
        To take these default arguments, the task can either list the ones
        it wants explicitly or just take an arbitrary list of keyword
        arguments (\*\*kwargs).

        """
        raise NotImplementedError("Tasks must define the run method.")

    def get_logger(self, loglevel=None, logfile=None, **kwargs):
        """Get process-aware logger object.

        See :func:`celery.log.setup_logger`.

        """
        return setup_logger(loglevel=loglevel, logfile=logfile)

    def establish_connection(self,
            connect_timeout=conf.AMQP_CONNECTION_TIMEOUT):
        """Establish a connection to the message broker."""
        return _establish_connection(connect_timeout)

    def get_publisher(self, connection=None, exchange=None,
            connect_timeout=conf.AMQP_CONNECTION_TIMEOUT):
        """Get a celery task message publisher.

        :rtype: :class:`celery.messaging.TaskPublisher`.

        Please be sure to close the AMQP connection when you're done
        with this object, i.e.:

            >>> publisher = self.get_publisher()
            >>> # do something with publisher
            >>> publisher.connection.close()

        """
        if exchange is None:
            exchange = self.exchange
        connection = connection or self.establish_connection(connect_timeout)
        return TaskPublisher(connection=connection,
                             exchange=exchange,
                             routing_key=self.routing_key)

    def get_consumer(self, connection=None,
            connect_timeout=conf.AMQP_CONNECTION_TIMEOUT):
        """Get a celery task message consumer.

        :rtype: :class:`celery.messaging.TaskConsumer`.

        Please be sure to close the AMQP connection when you're done
        with this object. i.e.:

            >>> consumer = self.get_consumer()
            >>> # do something with consumer
            >>> consumer.connection.close()

        """
        connection = connection or self.establish_connection(connect_timeout)
        return TaskConsumer(connection=connection, exchange=self.exchange,
                            routing_key=self.routing_key)

    @classmethod
    def delay(cls, *args, **kwargs):
        """Shortcut to :meth:`apply_async`, with star arguments,
        but doesn't support the extra options.

        :param \*args: positional arguments passed on to the task.
        :param \*\*kwargs: keyword arguments passed on to the task.

        :returns: :class:`celery.result.AsyncResult`

        """
        return cls.apply_async(args, kwargs)

    @classmethod
    def apply_async(cls, args=None, kwargs=None, **options):
        """Delay this task for execution by the ``celery`` daemon(s).

        :param args: positional arguments passed on to the task.
        :param kwargs: keyword arguments passed on to the task.
        :keyword \*\*options: Any keyword arguments to pass on to
            :func:`celery.execute.apply_async`.

        See :func:`celery.execute.apply_async` for more information.

        :rtype: :class:`celery.result.AsyncResult`


        """
        return apply_async(cls, args, kwargs, **options)

    def retry(self, args, kwargs, exc=None, throw=True, **options):
        """Retry the task.

        :param args: Positional arguments to retry with.
        :param kwargs: Keyword arguments to retry with.
        :keyword exc: Optional exception to raise instead of
            :exc:`MaxRestartsExceededError` when the max restart limit has
            been exceeded.
        :keyword countdown: Time in seconds to delay the retry for.
        :keyword eta: Explicit time and date to run the retry at (must be a
            :class:`datetime.datetime` instance).
        :keyword throw: If this is ``False``, do not raise the
            :exc:`celery.exceptions.RetryTaskError` exception,
            that tells the worker that the task is to be retried.
        :keyword \*\*options: Any extra options to pass on to
            meth:`apply_async`. See :func:`celery.execute.apply_async`.

        :raises celery.exceptions.RetryTaskError: To tell the worker that the
            task has been re-sent for retry. This always happens except if
            the ``throw`` keyword argument has been explicitly set
            to ``False``.

        Example

            >>> class TwitterPostStatusTask(Task):
            ...
            ...     def run(self, username, password, message, **kwargs):
            ...         twitter = Twitter(username, password)
            ...         try:
            ...             twitter.post_status(message)
            ...         except twitter.FailWhale, exc:
            ...             # Retry in 5 minutes.
            ...             self.retry([username, password, message], kwargs,
            ...                        countdown=60 * 5, exc=exc)

        """
        options["retries"] = kwargs.pop("task_retries", 0) + 1
        options["task_id"] = kwargs.pop("task_id", None)
        options["countdown"] = options.get("countdown",
                                           self.default_retry_delay)
        max_exc = exc or self.MaxRetriesExceededError(
                "Can't retry %s[%s] args:%s kwargs:%s" % (
                    self.name, options["task_id"], args, kwargs))
        if options["retries"] > self.max_retries:
            raise max_exc

        # If task was executed eagerly using apply(),
        # then the retry must also be executed eagerly.
        if kwargs.get("task_is_eager", False):
            result = self.apply(args=args, kwargs=kwargs, **options)
            if isinstance(result, EagerResult):
                return result.get() # propogates exceptions.
            return result

        self.apply_async(args=args, kwargs=kwargs, **options)

        if throw:
            message = "Retry in %d seconds." % options["countdown"]
            raise RetryTaskError(message, exc)

    def on_retry(self, exc, task_id, args, kwargs):
        """Retry handler.

        This is run by the worker when the task is to be retried.

        :param exc: The exception sent to :meth:`retry`.
        :param task_id: Unique id of the retried task.
        :param args: Original arguments for the retried task.
        :param kwargs: Original keyword arguments for the retried task.

        The return value of this handler is ignored.

        """
        pass

    def on_failure(self, exc, task_id, args, kwargs):
        """Error handler.

        This is run by the worker when the task fails.

        :param exc: The exception raised by the task.
        :param task_id: Unique id of the failed task.
        :param args: Original arguments for the task that failed.
        :param kwargs: Original keyword arguments for the task that failed.

        The return value of this handler is ignored.

        """
        pass

    def on_success(self, retval, task_id, args, kwargs):
        """Success handler.

        Run by the worker if the task executes successfully.

        :param retval: The return value of the task.
        :param task_id: Unique id of the executed task.
        :param args: Original arguments for the executed task.
        :param kwargs: Original keyword arguments for the executed task.

        The return value of this handler is ignored.

        """
        pass

    @classmethod
    def apply(cls, args=None, kwargs=None, **options):
        """Execute this task at once, by blocking until the task
        has finished executing.

        :param args: positional arguments passed on to the task.
        :param kwargs: keyword arguments passed on to the task.
        :rtype: :class:`celery.result.EagerResult`

        See :func:`celery.execute.apply`.

        """
        return apply(cls, args, kwargs, **options)


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
        return pickle.loads(ser_callable)(*fargs, **fkwargs)


class AsynchronousMapTask(Task):
    """Task used internally by :func:`dmap_async` and
    :meth:`TaskSet.map_async`.  """
    name = "celery.map_async"

    def run(self, ser_callable, args, timeout=None, **kwargs):
        """:see :meth:`TaskSet.dmap_async`."""
        return TaskSet.map(pickle.loads(ser_callable), args, timeout=timeout)


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
        ...                 ([], {"feed_url": "http://cnn.com/rss"}),
        ...                 ([], {"feed_url": "http://bbc.com/rss"}),
        ...                 ([], {"feed_url": "http://xkcd.com/rss"})
        ... ])

        >>> taskset_result = taskset.apply_async()
        >>> list_of_return_values = taskset_result.join()

    """

    def __init__(self, task, args):
        try:
            task_name = task.name
            task_obj = task
        except AttributeError:
            task_name = task
            task_obj = tasks[task_name]

        # Get task instance
        task_obj = tasks[task_obj.name]

        self.task = task_obj
        self.task_name = task_name
        self.arguments = args
        self.total = len(args)

    def run(self, *args, **kwargs):
        """Deprecated alias to :meth:`apply_async`"""
        warnings.warn(PendingDeprecationWarning(
            "TaskSet.run will be deprecated in favor of TaskSet.apply_async "
            "in celery v1.2.0"))
        return self.apply_async(*args, **kwargs)

    def apply_async(self, connect_timeout=conf.AMQP_CONNECTION_TIMEOUT):
        """Run all tasks in the taskset.

        :returns: A :class:`celery.result.TaskSetResult` instance.

        Example

            >>> ts = TaskSet(RefreshFeedTask, args=[
            ...         (["http://foo.com/rss"], {}),
            ...         (["http://bar.com/rss"], {}),
            ... ])
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
        if conf.ALWAYS_EAGER:
            return self.apply()

        taskset_id = gen_unique_id()

        conn = self.task.establish_connection(connect_timeout=connect_timeout)
        publisher = self.task.get_publisher(connection=conn)
        try:
            subtasks = [apply_async(self.task, args, kwargs,
                                    taskset_id=taskset_id, publisher=publisher)
                            for args, kwargs in self.arguments]
        finally:
            publisher.close()
            conn.close()
        return TaskSetResult(taskset_id, subtasks)

    def apply(self):
        taskset_id = gen_unique_id()
        subtasks = [apply(self.task, args, kwargs)
                        for args, kwargs in self.arguments]
        return TaskSetResult(taskset_id, subtasks)

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
        return remote_task.run().join(timeout=timeout)

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

    Results of periodic tasks are not stored by default.

    .. attribute:: run_every

        *REQUIRED* Defines how often the task is run (its interval),
        it can be either a :class:`datetime.timedelta` object or an
        integer specifying the time in seconds.

    :raises NotImplementedError: if the :attr:`run_every` attribute is
        not defined.

    Example

        >>> from celery.task import tasks, PeriodicTask
        >>> from datetime import timedelta
        >>> class MyPeriodicTask(PeriodicTask):
        ...     run_every = timedelta(seconds=30)
        ...
        ...     def run(self, **kwargs):
        ...         logger = self.get_logger(**kwargs)
        ...         logger.info("Running MyPeriodicTask")

    """
    abstract = True
    ignore_result = True
    type = "periodic"

    def __init__(self):
        if not hasattr(self, "run_every"):
            raise NotImplementedError(
                    "Periodic tasks must have a run_every attribute")

        # If run_every is a integer, convert it to timedelta seconds.
        # Operate on the original class attribute so anyone accessing
        # it directly gets the right value.
        if isinstance(self.__class__.run_every, int):
            self.__class__.run_every = timedelta(seconds=self.run_every)

        super(PeriodicTask, self).__init__()

    def remaining_estimate(self, last_run_at):
        """Returns when the periodic task should run next as a timedelta."""
        return (last_run_at + self.run_every) - datetime.now()

    def timedelta_seconds(self, delta):
        """Convert :class:`datetime.timedelta` to seconds.

        Doesn't account for negative timedeltas.

        """
        if delta.days < 0:
            return 0
        return delta.days * 86400 + delta.seconds + (delta.microseconds / 10e5)

    def is_due(self, last_run_at):
        """Returns tuple of two items ``(is_due, next_time_to_run)``,
        where next time to run is in seconds.

        e.g.

        * ``(True, 20)``, means the task should be run now, and the next
            time to run is in 20 seconds.

        * ``(False, 12)``, means the task should be run in 12 seconds.

        You can override this to decide the interval at runtime,
        but keep in mind the value of ``CELERYBEAT_MAX_LOOP_INTERVAL``, which
        decides the maximum number of seconds celerybeat can sleep between
        re-checking the periodic task intervals. So if you dynamically change
        the next run at value, and the max interval is set to 5 minutes, it
        will take 5 minutes for the change to take effect, so you may
        consider lowering the value of ``CELERYBEAT_MAX_LOOP_INTERVAL`` if
        responsiveness if of importance to you.

        """
        rem_delta = self.remaining_estimate(last_run_at)
        rem = self.timedelta_seconds(rem_delta)
        if rem == 0:
            return True, self.timedelta_seconds(self.run_every)
        return False, rem
