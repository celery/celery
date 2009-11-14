import sys
from datetime import timedelta

from carrot.connection import DjangoBrokerConnection

from celery import conf
from celery.log import setup_logger
from celery.utils import gen_unique_id, get_full_cls_name
from celery.result import TaskSetResult, EagerResult
from celery.execute import apply_async, apply
from celery.registry import tasks
from celery.backends import default_backend
from celery.messaging import TaskPublisher, TaskConsumer
from celery.exceptions import MaxRetriesExceededError, RetryTaskError
from celery.serialization import pickle


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
        if attrs.pop("abstract", None):
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
    """A task that can be delayed for execution by the ``celery`` daemon.

    All subclasses of :class:`Task` must define the :meth:`run` method,
    which is the actual method the ``celery`` daemon executes.

    The :meth:`run` method can take use of the default keyword arguments,
    as listed in the :meth:`run` documentation.

    The :meth:`run` method supports both positional, and keyword arguments.

    .. attribute:: name

        *REQUIRED* All subclasses of :class:`Task` has to define the
        :attr:`name` attribute. This is the name of the task, registered
        in the task registry, and passed on to the workers.

    .. attribute:: abstract

        Abstract classes are not registered in the task registry, so they're
        only used for making new tasks by subclassing.

    .. attribute:: type

        The type of task, currently this can be ``regular``, or ``periodic``,
        however if you want a periodic task, you should subclass
        :class:`PeriodicTask` instead.

    .. attribute:: routing_key

        Override the global default ``routing_key`` for this task.

    .. attribute:: exchange

        Override the global default ``exchange`` for this task.

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

    .. attribute:: max_retries

        Maximum number of retries before giving up.

    .. attribute:: default_retry_delay

        Defeault time in seconds before a retry of the task should be
        executed. Default is a 1 minute delay.

    .. rate_limit:: Set the rate limit for this task type,
        if this is ``None`` no rate limit is in effect.
        The rate limits can be specified in seconds, minutes or hours
        by appending ``"/s"``, ``"/m"`` or "``/h"``". If this is an integer
        it is interpreted as seconds. Example: ``"100/m" (hundred tasks a
        minute). Default is the ``CELERY_DEFAULT_RATE_LIMIT`` setting (which
        is off if not specified).

    .. attribute:: ignore_result

        Don't store the status and return value. This means you can't
        use the :class:`celery.result.AsyncResult` to check if the task is
        done, or get its return value. Only use if you need the performance
        and is able live without these features. Any exceptions raised will
        store the return value/status as usual.

    .. attribute:: disable_error_emails

        Disable all error e-mails for this task (only applicable if
        ``settings.SEND_CELERY_ERROR_EMAILS`` is on.)

    .. attribute:: serializer

        A string identifying the default serialization
        method to use. Defaults to the ``CELERY_TASK_SERIALIZER`` setting.
        Can be ``pickle`` ``json``, ``yaml``, or any custom serialization
        methods that have been registered with
        :mod:`carrot.serialization.registry`.

    :raises NotImplementedError: if the :attr:`name` attribute is not set.

    The resulting class is callable, which if called will apply the
    :meth:`run` method.

    Examples

    This is a simple task just logging a message,

        >>> from celery.task import tasks, Task
        >>> class MyTask(Task):
        ...
        ...     def run(self, some_arg=None, **kwargs):
        ...         logger = self.get_logger(**kwargs)
        ...         logger.info("Running MyTask with arg some_arg=%s" %
        ...                     some_arg))
        ...         return 42

    You can delay the task using the classmethod :meth:`delay`...

        >>> result = MyTask.delay(some_arg="foo")
        >>> result.status # after some time
        'SUCCESS'
        >>> result.result
        42


    """
    __metaclass__ = TaskType

    name = None
    abstract = True
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

                Unique id of the currently executing task.

            * task_name

                Name of the currently executing task (same as :attr:`name`)

            * task_retries

                How many times the current task has been retried
                (an integer starting at ``0``).

            * logfile

                Name of the worker log file.

            * loglevel

                The current loglevel, an integer mapping to one of the
                following values: ``logging.DEBUG``, ``logging.INFO``,
                ``logging.ERROR``, ``logging.CRITICAL``, ``logging.WARNING``,
                ``logging.FATAL``.

        Additional standard keyword arguments may be added in the future.
        To take these default arguments, the task can either list the ones
        it wants explicitly or just take an arbitrary list of keyword
        arguments (\*\*kwargs).

        Example using an explicit list of default arguments to take:

        .. code-block:: python

            def run(self, x, y, logfile=None, loglevel=None):
                self.get_logger(loglevel=loglevel, logfile=logfile)
                return x * y


        Example taking all default keyword arguments, and any extra arguments
        passed on by the caller:

        .. code-block:: python

            def run(self, x, y, **kwargs): # CORRECT!
                logger = self.get_logger(**kwargs)
                adjust = kwargs.get("adjust", 0)
                return x * y - adjust

        """
        raise NotImplementedError("Tasks must define a run method.")

    def get_logger(self, **kwargs):
        """Get process-aware logger object.

        See :func:`celery.log.setup_logger`.

        """
        logfile = kwargs.get("logfile")
        loglevel = kwargs.get("loglevel")
        return setup_logger(loglevel=loglevel, logfile=logfile)

    def get_publisher(self, connect_timeout=conf.AMQP_CONNECTION_TIMEOUT):
        """Get a celery task message publisher.

        :rtype: :class:`celery.messaging.TaskPublisher`.

        Please be sure to close the AMQP connection when you're done
        with this object, i.e.:

            >>> publisher = self.get_publisher()
            >>> # do something with publisher
            >>> publisher.connection.close()

        """

        connection = DjangoBrokerConnection(connect_timeout=connect_timeout)
        return TaskPublisher(connection=connection,
                             exchange=self.exchange,
                             routing_key=self.routing_key)

    def get_consumer(self, connect_timeout=conf.AMQP_CONNECTION_TIMEOUT):
        """Get a celery task message consumer.

        :rtype: :class:`celery.messaging.TaskConsumer`.

        Please be sure to close the AMQP connection when you're done
        with this object. i.e.:

            >>> consumer = self.get_consumer()
            >>> # do something with consumer
            >>> consumer.connection.close()

        """
        connection = DjangoBrokerConnection(connect_timeout=connect_timeout)
        return TaskConsumer(connection=connection, exchange=self.exchange,
                            routing_key=self.routing_key)

    @classmethod
    def delay(cls, *args, **kwargs):
        """Shortcut to :meth:`apply_async` but with star arguments,
        and doesn't support the extra options.

        :param \*args: positional arguments passed on to the task.

        :param \*\*kwargs: keyword arguments passed on to the task.

        :rtype: :class:`celery.result.AsyncResult`

        """
        return apply_async(cls, args, kwargs)

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
        :keyword throw: Do not raise the
            :exc:`celery.exceptions.RetryTaskError` exception,
            that tells the worker that the task is to be retried.
        :keyword countdown: Time in seconds to delay the retry for.
        :keyword eta: Explicit time and date to run the retry at (must be a
            :class:`datetime.datetime` instance).
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
                # get() propogates any exceptions.
                return result.get()
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

        This is run by the worker when the task executed successfully.

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
        callable_ = pickle.loads(ser_callable)
        return callable_(*fargs, **fkwargs)


class AsynchronousMapTask(Task):
    """Task used internally by :func:`dmap_async` and
    :meth:`TaskSet.map_async`.  """
    name = "celery.map_async"

    def run(self, serfunc, args, **kwargs):
        """The method run by ``celeryd``."""
        timeout = kwargs.get("timeout")
        return TaskSet.map(pickle.loads(serfunc), args, timeout=timeout)


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

    def run(self, connect_timeout=conf.AMQP_CONNECTION_TIMEOUT):
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
        taskset_id = gen_unique_id()

        from celery.conf import ALWAYS_EAGER
        if ALWAYS_EAGER:
            subtasks = [apply(self.task, args, kwargs)
                            for args, kwargs in self.arguments]
            return TaskSetResult(taskset_id, subtasks)

        conn = DjangoBrokerConnection(connect_timeout=connect_timeout)
        publisher = TaskPublisher(connection=conn,
                                  exchange=self.task.exchange)
        subtasks = [apply_async(self.task, args, kwargs,
                                taskset_id=taskset_id, publisher=publisher)
                        for args, kwargs in self.arguments]
        publisher.close()
        conn.close()
        return TaskSetResult(taskset_id, subtasks)

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

    """
    abstract = True
    run_every = timedelta(days=1)
    ignore_result = True
    type = "periodic"

    def __init__(self):
        if not self.run_every:
            raise NotImplementedError(
                    "Periodic tasks must have a run_every attribute")

        # If run_every is a integer, convert it to timedelta seconds.
        # Operate on the original class attribute so anyone accessing
        # it directly gets the right value.
        if isinstance(self.__class__.run_every, int):
            self.__class__.run_every = timedelta(seconds=self.run_every)

        super(PeriodicTask, self).__init__()
