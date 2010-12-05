import sys
import threading
import warnings

from celery.app import app_or_default
from celery.datastructures import ExceptionInfo
from celery.exceptions import MaxRetriesExceededError, RetryTaskError
from celery.execute.trace import TaskTrace
from celery.registry import tasks, _unpickle_task
from celery.result import EagerResult
from celery.schedules import maybe_schedule
from celery.utils import mattrgetter, gen_unique_id, fun_takes_kwargs
from celery.utils.timeutils import timedelta_seconds

from celery.task import sets

TaskSet = sets.TaskSet
subtask = sets.subtask

PERIODIC_DEPRECATION_TEXT = """\
Periodic task classes has been deprecated and will be removed
in celery v3.0.

Please use the CELERYBEAT_SCHEDULE setting instead:

    CELERYBEAT_SCHEDULE = {
        name: dict(task=task_name, schedule=run_every,
                   args=(), kwargs={}, options={}, relative=False)
    }

"""
extract_exec_options = mattrgetter("queue", "routing_key",
                                   "exchange", "immediate",
                                   "mandatory", "priority",
                                   "serializer", "delivery_mode",
                                   "compression")
_default_context = {"logfile": None,
                    "loglevel": None,
                    "id": None,
                    "args": None,
                    "kwargs": None,
                    "retries": 0,
                    "is_eager": False,
                    "delivery_info": None}


class Context(threading.local):

    def update(self, d, **kwargs):
        self.__dict__.update(d, **kwargs)

    def clear(self):
        self.__dict__.clear()
        self.update(_default_context)

    def get(self, key, default=None):
        return self.__dict__.get(key, default)


class TaskType(type):
    """Metaclass for tasks.

    Automatically registers the task in the task registry, except
    if the `abstract` attribute is set.

    If no `name` attribute is provided, the name is automatically
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
            task_name = ".".join([sys.modules[task_module].__name__, name])
            attrs["name"] = task_name

        # Because of the way import happens (recursively)
        # we may or may not be the first time the task tries to register
        # with the framework. There should only be one class for each task
        # name, so we always return the registered version.

        task_name = attrs["name"]
        if task_name not in tasks:
            task_cls = super_new(cls, name, bases, attrs)
            if task_module == "__main__" and task_cls.app.main:
                task_name = task_cls.name = ".".join([task_cls.app.main,
                                                      name])
            tasks.register(task_cls)
        task = tasks[task_name].__class__
        return task


class BaseTask(object):
    """A Celery task.

    All subclasses of :class:`Task` must define the :meth:`run` method,
    which is the actual method the `celery` daemon executes.

    The :meth:`run` method can take use of the default keyword arguments,
    as listed in the :meth:`run` documentation.

    The resulting class is callable, which if called will apply the
    :meth:`run` method.

    """
    __metaclass__ = TaskType

    MaxRetriesExceededError = MaxRetriesExceededError

    #: The application instance associated with this task class.
    app = None

    #: Name of the task.
    name = None

    #: If :const:`True` the task is an abstract base class.
    abstract = True

    #: If disabled the worker will not forward magic keyword arguments.
    accept_magic_kwargs = True

    #: Current request context (when task is executed).
    request = Context()

    #: Destination queue.  The queue needs to exist
    #: in :setting:`CELERY_QUEUES`.  The `routing_key`, `exchange` and
    #: `exchange_type` attributes will be ignored if this is set.
    queue = None

    #: Overrides the apps default `routing_key` for this task.
    routing_key = None

    #: Overrides the apps default `exchange` for this task.
    exchange = None

    #: Overrides the apps default exchange type for this task.
    exchange_type = None

    #: Override the apps default delivery mode for this task.  Default is
    #: `"persistent"`, but you can change this to `"transient"`, which means
    #: messages will be lost if the broker is restarted.  Consult your broker
    #: manual for any additional delivery modes.
    delivery_mode = None

    #: Mandatory message routing.
    mandatory = False

    #: Request immediate delivery.
    immediate = False

    #: Default message priority.  A number between 0 to 9, where 0 is the
    #: highest.  Note that RabbitMQ does not support priorities.
    priority = None

    #: Maximum number of retries before giving up.  If set to :const:`None`,
    #: it will **never** stop retrying.
    max_retries = 3

    #: Default time in seconds before a retry of the task should be
    #: executed.  3 minutes by default.
    default_retry_delay = 3 * 60

    #: Rate limit for this task type.  Examples: :const:`None` (no rate
    #: limit), `"100/s"` (hundred tasks a second), `"100/m"` (hundred tasks
    #: a minute),`"100/h"` (hundred tasks an hour)
    rate_limit = None

    #: If enabled the worker will not store task state and return values
    #: for this task.  Defaults to the :setting:`CELERY_IGNORE_RESULT`
    #: setting.
    ignore_result = False

    #: When enabled errors will be stored even if the task is otherwise
    #: configured to ignore results.
    store_errors_even_if_ignored = False

    #: If enabled an e-mail will be sent to :setting:`ADMINS` whenever a task
    #: of this type fails.
    send_error_emails = False

    disable_error_emails = False                            # FIXME

    #: List of exception types to send error e-mails for.
    error_whitelist = ()

    #: The name of a serializer that has been registered with
    #: :mod:`kombu.serialization.registry`.  Default is `"pickle"`.
    serializer = "pickle"

    #: The result store backend used for this task.
    backend = None

    #: If disabled the task will not be automatically registered
    #: in the task registry.
    autoregister = True

    #: If enabled the task will report its status as "started" when the task
    #: is executed by a worker.  Disabled by default as the normal behaviour
    #: is to not report that level of granularity.  Tasks are either pending,
    #: finished, or waiting to be retried.
    #:
    #: Having a "started" status can be useful for when there are long
    #: running tasks and there is a need to report which task is currently
    #: running.
    #:
    #: The application default can be overridden using the
    #: :setting:`CELERY_TRACK_STARTED` setting.
    track_started = False

    #: When enabled  messages for this task will be acknowledged **after**
    #: the task has been executed, and not *just before* which is the
    #: default behavior.
    #:
    #: Please note that this means the task may be executed twice if the
    #: worker crashes mid execution (which may be acceptable for some
    #: applications).
    #:
    #: The application default can be overriden with the
    #: :setting:`CELERY_ACKS_LATE` setting.
    acks_late = False

    #: Default task expiry time.
    expires = None

    #: The type of task *(no longer used)*.
    type = "regular"

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def __reduce__(self):
        return (_unpickle_task, (self.name, ), None)

    def run(self, *args, **kwargs):
        """The body of the task executed by the worker.

        The following standard keyword arguments are reserved and is
        automatically passed by the worker if the function/method
        supports them:

            * `task_id`
            * `task_name`
            * `task_retries
            * `task_is_eager`
            * `logfile`
            * `loglevel`
            * `delivery_info`

        To take these default arguments, the task can either list the ones
        it wants explicitly or just take an arbitrary list of keyword
        arguments (\*\*kwargs).

        Magic keyword arguments can be disabled using the
        :attr:`accept_magic_kwargs` flag.  The information can then
        be found in the :attr:`request` attribute.

        """
        raise NotImplementedError("Tasks must define the run method.")

    @classmethod
    def get_logger(self, loglevel=None, logfile=None, propagate=False,
            **kwargs):
        """Get task-aware logger object.

        See :func:`celery.log.setup_task_logger`.

        """
        if loglevel is None:
            loglevel = self.request.loglevel
        if logfile is None:
            logfile = self.request.logfile
        return self.app.log.setup_task_logger(loglevel=loglevel,
                                              logfile=logfile,
                                              propagate=propagate,
                                              task_kwargs=self.request.kwargs)

    @classmethod
    def establish_connection(self, connect_timeout=None):
        """Establish a connection to the message broker."""
        return self.app.broker_connection(connect_timeout=connect_timeout)

    @classmethod
    def get_publisher(self, connection=None, exchange=None,
            connect_timeout=None, exchange_type=None):
        """Get a celery task message publisher.

        :rtype :class:`~celery.app.amqp.TaskPublisher`:

        Please be sure to close the AMQP connection after you're done
        with this object.  Example::

            >>> publisher = self.get_publisher()
            >>> # ... do something with publisher
            >>> publisher.connection.close()

        """
        if exchange is None:
            exchange = self.exchange
        if exchange_type is None:
            exchange_type = self.exchange_type
        connection = connection or self.establish_connection(connect_timeout)
        return self.app.amqp.TaskPublisher(connection=connection,
                                           exchange=exchange,
                                           exchange_type=exchange_type,
                                           routing_key=self.routing_key)

    @classmethod
    def get_consumer(self, connection=None, connect_timeout=None):
        """Get message consumer.

        :rtype :class:`~celery.app.amqp.TaskConsumer`:

        Please be sure to close the AMQP connection when you're done
        with this object.  Example::

            >>> consumer = self.get_consumer()
            >>> # do something with consumer
            >>> consumer.connection.close()

        """
        connection = connection or self.establish_connection(connect_timeout)
        return self.app.amqp.TaskConsumer(connection=connection,
                                          exchange=self.exchange,
                                          routing_key=self.routing_key)

    @classmethod
    def delay(self, *args, **kwargs):
        """Shortcut to :meth:`apply_async` giving star arguments, but without
        options.

        :param \*args: positional arguments passed on to the task.
        :param \*\*kwargs: keyword arguments passed on to the task.

        :returns :class:`celery.result.AsyncResult`:

        """
        return self.apply_async(args, kwargs)

    @classmethod
    def apply_async(self, args=None, kwargs=None, countdown=None,
            eta=None, task_id=None, publisher=None, connection=None,
            connect_timeout=None, router=None, expires=None, queues=None,
            **options):
        """Run a task asynchronously by the celery daemon(s).

        :keyword args: The positional arguments to pass on to the
                       task (a :class:`list` or :class:`tuple`).

        :keyword kwargs: The keyword arguments to pass on to the
                         task (a :class:`dict`)

        :keyword countdown: Number of seconds into the future that the
                            task should execute. Defaults to immediate
                            delivery (do not confuse with the
                            `immediate` flag, as they are unrelated).

        :keyword eta: A :class:`~datetime.datetime` object describing
                      the absolute time and date of when the task should
                      be executed.  May not be specified if `countdown`
                      is also supplied.  (Do not confuse this with the
                      `immediate` flag, as they are unrelated).

        :keyword expires: Either a :class:`int`, describing the number of
                          seconds, or a :class:`~datetime.datetime` object
                          that describes the absolute time and date of when
                          the task should expire.  The task will not be
                          executed after the expiration time.

        :keyword connection: Re-use existing broker connection instead
                             of establishing a new one.  The `connect_timeout`
                             argument is not respected if this is set.

        :keyword connect_timeout: The timeout in seconds, before we give up
                                  on establishing a connection to the AMQP
                                  server.

        :keyword routing_key: The routing key used to route the task to a
                              worker server.  Defaults to the
                              :attr:`routing_key` attribute.

        :keyword exchange: The named exchange to send the task to.
                           Defaults to the :attr:`exchange` attribute.

        :keyword exchange_type: The exchange type to initalize the exchange
                                if not already declared.  Defaults to the
                                :attr:`exchange_type` attribute.

        :keyword immediate: Request immediate delivery.  Will raise an
                            exception if the task cannot be routed to a worker
                            immediately.  (Do not confuse this parameter with
                            the `countdown` and `eta` settings, as they are
                            unrelated).  Defaults to the :attr:`immediate`
                            attribute.

        :keyword mandatory: Mandatory routing. Raises an exception if
                            there's no running workers able to take on this
                            task.  Defaults to the :attr:`mandatory`
                            attribute.

        :keyword priority: The task priority, a number between 0 and 9.
                           Defaults to the :attr:`priority` attribute.

        :keyword serializer: A string identifying the default
                             serialization method to use.  Can be `pickle`,
                             `json`, `yaml`, `msgpack` or any custom
                             serialization method that has been registered
                             with :mod:`kombu.serialization.registry`.
                             Defaults to the :attr:`serializer` attribute.

        :keyword compression: A string identifying the compression method
                              to use.  Can be one of ``zlib``, ``bzip2``,
                              or any custom compression methods registered with
                              :func:`kombu.compression.register`. Defaults to
                              the :setting:`CELERY_MESSAGE_COMPRESSION`
                              setting.

        .. note::
            If the :setting:`CELERY_ALWAYS_EAGER` setting is set, it will
            be replaced by a local :func:`apply` call instead.

        """
        router = self.app.amqp.Router(queues)
        conf = self.app.conf

        if conf.CELERY_ALWAYS_EAGER:
            return self.apply(args, kwargs, task_id=task_id)

        options.setdefault("compression",
                           conf.CELERY_MESSAGE_COMPRESSION)
        options = dict(extract_exec_options(self), **options)
        options = router.route(options, self.name, args, kwargs)
        exchange = options.get("exchange")
        exchange_type = options.get("exchange_type")
        expires = expires or self.expires

        publish = publisher or self.get_publisher(connection,
                                                  exchange=exchange,
                                                  exchange_type=exchange_type)
        evd = None
        if conf.CELERY_SEND_TASK_SENT_EVENT:
            evd = self.app.events.Dispatcher(channel=publish.channel,
                                             buffer_while_offline=False)

        try:
            task_id = publish.delay_task(self.name, args, kwargs,
                                         task_id=task_id,
                                         countdown=countdown,
                                         eta=eta, expires=expires,
                                         event_dispatcher=evd,
                                         **options)
        finally:
            if not publisher:
                publish.close()
                publish.connection.close()

        return self.AsyncResult(task_id)

    @classmethod
    def retry(self, args=None, kwargs=None, exc=None, throw=True,
            **options):
        """Retry the task.

        :param args: Positional arguments to retry with.
        :param kwargs: Keyword arguments to retry with.
        :keyword exc: Optional exception to raise instead of
                      :exc:`~celery.exceptions.MaxRetriesExceededError`
                      when the max restart limit has been exceeded.
        :keyword countdown: Time in seconds to delay the retry for.
        :keyword eta: Explicit time and date to run the retry at
                      (must be a :class:`~datetime.datetime` instance).
        :keyword \*\*options: Any extra options to pass on to
                              meth:`apply_async`.
        :keyword throw: If this is :const:`False`, do not raise the
                        :exc:`~celery.exceptions.RetryTaskError` exception,
                        that tells the worker to mark the task as being
                        retried.  Note that this means the task will be
                        marked as failed if the task raises an exception,
                        or successful if it returns.

        :raises celery.exceptions.RetryTaskError: To tell the worker that
            the task has been re-sent for retry. This always happens,
            unless the `throw` keyword argument has been explicitly set
            to :const:`False`, and is considered normal operation.

        **Example**

        .. code-block:: python

            >>> @task
            >>> def tweet(auth, message):
            ...     twitter = Twitter(oauth=auth)
            ...     try:
            ...         twitter.post_status_update(message)
            ...     except twitter.FailWhale, exc:
            ...         # Retry in 5 minutes.
            ...         return tweet.retry(countdown=60 * 5, exc=exc)

        Although the task will never return above as `retry` raises an
        exception to notify the worker, we use `return` in front of the retry
        to convey that the rest of the block will not be executed.

        """
        request = self.request
        if args is None:
            args = request.args
        if kwargs is None:
            kwargs = request.kwargs

        delivery_info = request.delivery_info
        if delivery_info:
            options.setdefault("exchange", delivery_info.get("exchange"))
            options.setdefault("routing_key", delivery_info.get("routing_key"))

        options["retries"] = request.retries + 1
        options["task_id"] = request.id
        options["countdown"] = options.get("countdown",
                                           self.default_retry_delay)
        max_exc = exc or self.MaxRetriesExceededError(
                "Can't retry %s[%s] args:%s kwargs:%s" % (
                    self.name, options["task_id"], args, kwargs))
        max_retries = self.max_retries
        if max_retries is not None and options["retries"] > max_retries:
            raise max_exc

        # If task was executed eagerly using apply(),
        # then the retry must also be executed eagerly.
        if request.is_eager:
            result = self.apply(args=args, kwargs=kwargs, **options)
            if isinstance(result, EagerResult):
                return result.get()             # propogates exceptions.
            return result

        self.apply_async(args=args, kwargs=kwargs, **options)

        if throw:
            message = "Retry in %d seconds." % options["countdown"]
            raise RetryTaskError(message, exc)

    @classmethod
    def apply(self, args=None, kwargs=None, **options):
        """Execute this task locally, by blocking until the task
        returns.

        :param args: positional arguments passed on to the task.
        :param kwargs: keyword arguments passed on to the task.
        :keyword throw: Re-raise task exceptions.  Defaults to
                        the :setting:`CELERY_EAGER_PROPAGATES_EXCEPTIONS`
                        setting.

        :rtype :class:`celery.result.EagerResult`:

        """
        args = args or []
        kwargs = kwargs or {}
        task_id = options.get("task_id") or gen_unique_id()
        retries = options.get("retries", 0)
        throw = self.app.either("CELERY_EAGER_PROPAGATES_EXCEPTIONS",
                                options.pop("throw", None))

        # Make sure we get the task instance, not class.
        task = tasks[self.name]

        request = {"id": task_id,
                   "retries": retries,
                   "is_eager": True,
                   "logfile": options.get("logfile"),
                   "loglevel": options.get("loglevel", 0),
                   "delivery_info": {"is_eager": True}}
        if self.accept_magic_kwargs:
            default_kwargs = {"task_name": task.name,
                              "task_id": task_id,
                              "task_retries": retries,
                              "task_is_eager": True,
                              "logfile": options.get("logfile"),
                              "loglevel": options.get("loglevel", 0),
                              "delivery_info": {"is_eager": True}}
            supported_keys = fun_takes_kwargs(task.run, default_kwargs)
            extend_with = dict((key, val)
                                    for key, val in default_kwargs.items()
                                        if key in supported_keys)
            kwargs.update(extend_with)

        trace = TaskTrace(task.name, task_id, args, kwargs,
                          task=task, request=request, propagate=throw)
        retval = trace.execute()
        if isinstance(retval, ExceptionInfo):
            retval = retval.exception
        return EagerResult(task_id, retval, trace.status,
                           traceback=trace.strtb)

    @classmethod
    def AsyncResult(self, task_id):
        """Get AsyncResult instance for this kind of task.

        :param task_id: Task id to get result for.

        """
        return self.app.AsyncResult(task_id, backend=self.backend,
                                             task_name=self.name)

    def update_state(self, task_id=None, state=None, meta=None):
        """Update task state.

        :param task_id: Id of the task to update.
        :param state: New state (:class:`str`).
        :param meta: State metadata (:class:`dict`).

        """
        if task_id is None:
            task_id = self.request.id
        self.backend.store_result(task_id, meta, state)

    def on_retry(self, exc, task_id, args, kwargs, einfo=None):
        """Retry handler.

        This is run by the worker when the task is to be retried.

        :param exc: The exception sent to :meth:`retry`.
        :param task_id: Unique id of the retried task.
        :param args: Original arguments for the retried task.
        :param kwargs: Original keyword arguments for the retried task.

        :keyword einfo: :class:`~celery.datastructures.ExceptionInfo`
                        instance, containing the traceback.

        The return value of this handler is ignored.

        """
        pass

    def after_return(self, status, retval, task_id, args,
            kwargs, einfo=None):
        """Handler called after the task returns.

        :param status: Current task state.
        :param retval: Task return value/exception.
        :param task_id: Unique id of the task.
        :param args: Original arguments for the task that failed.
        :param kwargs: Original keyword arguments for the task
                       that failed.

        :keyword einfo: :class:`~celery.datastructures.ExceptionInfo`
                        instance, containing the traceback (if any).

        The return value of this handler is ignored.

        """
        pass

    def on_failure(self, exc, task_id, args, kwargs, einfo=None):
        """Error handler.

        This is run by the worker when the task fails.

        :param exc: The exception raised by the task.
        :param task_id: Unique id of the failed task.
        :param args: Original arguments for the task that failed.
        :param kwargs: Original keyword arguments for the task
                       that failed.

        :keyword einfo: :class:`~celery.datastructures.ExceptionInfo`
                        instance, containing the traceback.

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

    def execute(self, wrapper, pool, loglevel, logfile):
        """The method the worker calls to execute the task.

        :param wrapper: A :class:`~celery.worker.job.TaskRequest`.
        :param pool: A task pool.
        :param loglevel: Current loglevel.
        :param logfile: Name of the currently used logfile.

        """
        wrapper.execute_using_pool(pool, loglevel, logfile)

    def __repr__(self):
        """`repr(task)`"""
        try:
            kind = self.__class__.mro()[1].__name__
        except (AttributeError, IndexError):            # pragma: no cover
            kind = "%s(Task)" % self.__class__.__name__
        return "<%s: %s (%s)>" % (kind, self.name, self.type)

    @classmethod
    def subtask(cls, *args, **kwargs):
        """Returns :class:`~celery.task.sets.subtask` object for
        this task, wrapping arguments and execution options
        for a single task invocation."""
        return subtask(cls, *args, **kwargs)

    @property
    def __name__(self):
        return self.__class__.__name__


def create_task_cls(app):
    apps = [app]

    class Task(BaseTask):
        app = apps[0]
        backend = app.backend
        exchange_type = app.conf.CELERY_DEFAULT_EXCHANGE_TYPE
        delivery_mode = app.conf.CELERY_DEFAULT_DELIVERY_MODE
        send_error_emails = app.conf.CELERY_SEND_TASK_ERROR_EMAILS
        error_whitelist = app.conf.CELERY_TASK_ERROR_WHITELIST
        serializer = app.conf.CELERY_TASK_SERIALIZER
        rate_limit = app.conf.CELERY_DEFAULT_RATE_LIMIT
        track_started = app.conf.CELERY_TRACK_STARTED
        acks_late = app.conf.CELERY_ACKS_LATE
        ignore_result = app.conf.CELERY_IGNORE_RESULT
        store_errors_even_if_ignored = \
                app.conf.CELERY_STORE_ERRORS_EVEN_IF_IGNORED

    return Task


Task = create_task_cls(app_or_default())


class PeriodicTask(Task):
    """A periodic task is a task that behaves like a :manpage:`cron` job.

    Results of periodic tasks are not stored by default.

    .. attribute:: run_every

        *REQUIRED* Defines how often the task is run (its interval),
        it can be a :class:`~datetime.timedelta` object, a
        :class:`~celery.task.schedules.crontab` object or an integer
        specifying the time in seconds.

    .. attribute:: relative

        If set to :const:`True`, run times are relative to the time when the
        server was started. This was the previous behaviour, periodic tasks
        are now scheduled by the clock.

    :raises NotImplementedError: if the :attr:`run_every` attribute is
        not defined.

    Example

        >>> from celery.task import tasks, PeriodicTask
        >>> from datetime import timedelta
        >>> class EveryThirtySecondsTask(PeriodicTask):
        ...     run_every = timedelta(seconds=30)
        ...
        ...     def run(self, **kwargs):
        ...         logger = self.get_logger(**kwargs)
        ...         logger.info("Execute every 30 seconds")

        >>> from celery.task import PeriodicTask
        >>> from celery.task.schedules import crontab

        >>> class EveryMondayMorningTask(PeriodicTask):
        ...     run_every = crontab(hour=7, minute=30, day_of_week=1)
        ...
        ...     def run(self, **kwargs):
        ...         logger = self.get_logger(**kwargs)
        ...         logger.info("Execute every Monday at 7:30AM.")

        >>> class EveryMorningTask(PeriodicTask):
        ...     run_every = crontab(hours=7, minute=30)
        ...
        ...     def run(self, **kwargs):
        ...         logger = self.get_logger(**kwargs)
        ...         logger.info("Execute every day at 7:30AM.")

        >>> class EveryQuarterPastTheHourTask(PeriodicTask):
        ...     run_every = crontab(minute=15)
        ...
        ...     def run(self, **kwargs):
        ...         logger = self.get_logger(**kwargs)
        ...         logger.info("Execute every 0:15 past the hour every day.")

    """
    abstract = True
    ignore_result = True
    type = "periodic"
    relative = False

    def __init__(self):
        app = app_or_default()
        if not hasattr(self, "run_every"):
            raise NotImplementedError(
                    "Periodic tasks must have a run_every attribute")
        self.run_every = maybe_schedule(self.run_every, self.relative)

        # Periodic task classes is pending deprecation.
        warnings.warn(PendingDeprecationWarning(PERIODIC_DEPRECATION_TEXT))

        # For backward compatibility, add the periodic task to the
        # configuration schedule instead.
        app.conf.CELERYBEAT_SCHEDULE[self.name] = {
                "task": self.name,
                "schedule": self.run_every,
                "args": (),
                "kwargs": {},
                "options": {},
                "relative": self.relative,
        }

        super(PeriodicTask, self).__init__()

    def timedelta_seconds(self, delta):
        """Convert :class:`~datetime.timedelta` to seconds.

        Doesn't account for negative timedeltas.

        """
        return timedelta_seconds(delta)

    def is_due(self, last_run_at):
        """Returns tuple of two items `(is_due, next_time_to_run)`,
        where next time to run is in seconds.

        See :meth:`celery.schedules.schedule.is_due` for more information.

        """
        return self.run_every.is_due(last_run_at)

    def remaining_estimate(self, last_run_at):
        """Returns when the periodic task should run next as a timedelta."""
        return self.run_every.remaining_estimate(last_run_at)
