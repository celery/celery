# -*- coding: utf-8 -*-
"""
    celery.app.task
    ~~~~~~~~~~~~~~~

    Tasks Implementation.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""

from __future__ import absolute_import

import sys
import threading

from ... import states
from ...datastructures import ExceptionInfo
from ...exceptions import MaxRetriesExceededError, RetryTaskError
from ...execute.trace import eager_trace_task
from ...registry import tasks, _unpickle_task
from ...result import EagerResult
from ...utils import (fun_takes_kwargs, instantiate,
                      mattrgetter, uuid, maybe_reraise)
from ...utils.mail import ErrorMail

extract_exec_options = mattrgetter("queue", "routing_key",
                                   "exchange", "immediate",
                                   "mandatory", "priority",
                                   "serializer", "delivery_mode",
                                   "compression", "expires")


class Context(threading.local):
    # Default context
    logfile = None
    loglevel = None
    hostname = None
    id = None
    args = None
    kwargs = None
    retries = 0
    is_eager = False
    delivery_info = None
    taskset = None
    chord = None
    called_directly = True

    def update(self, d, **kwargs):
        self.__dict__.update(d, **kwargs)

    def clear(self):
        self.__dict__.clear()

    def get(self, key, default=None):
        try:
            return getattr(self, key)
        except AttributeError:
            return default

    def __repr__(self):
        return "<Context: %r>" % (vars(self, ))


class TaskType(type):
    """Meta class for tasks.

    Automatically registers the task in the task registry, except
    if the `abstract` attribute is set.

    If no `name` attribute is provided, then no name is automatically
    set to the name of the module it was defined in, and the class name.

    """

    def __new__(cls, name, bases, attrs):
        new = super(TaskType, cls).__new__
        task_module = attrs.get("__module__") or "__main__"

        if "__call__" in attrs:
            # see note about __call__ below.
            attrs["__defines_call__"] = True

        # - Abstract class: abstract attribute should not be inherited.
        if attrs.pop("abstract", None) or not attrs.get("autoregister", True):
            return new(cls, name, bases, attrs)

        # - Automatically generate missing/empty name.
        autoname = False
        if not attrs.get("name"):
            try:
                module_name = sys.modules[task_module].__name__
            except KeyError:  # pragma: no cover
                # Fix for manage.py shell_plus (Issue #366).
                module_name = task_module
            attrs["name"] = '.'.join([module_name, name])
            autoname = True

        # - Automatically generate __call__.
        # If this or none of its bases define __call__, we simply
        # alias it to the ``run`` method, as
        # this means we can skip a stacktrace frame :)
        if not (attrs.get("__call__")
                or any(getattr(b, "__defines_call__", False) for b in bases)):
            try:
                attrs["__call__"] = attrs["run"]
            except KeyError:

                # the class does not yet define run,
                # so we can't optimize this case.
                def __call__(self, *args, **kwargs):
                    return self.run(*args, **kwargs)
                attrs["__call__"] = __call__

        # - Create and register class.
        # Because of the way import happens (recursively)
        # we may or may not be the first time the task tries to register
        # with the framework.  There should only be one class for each task
        # name, so we always return the registered version.
        task_name = attrs["name"]
        if task_name not in tasks:
            task_cls = new(cls, name, bases, attrs)
            if autoname and task_module == "__main__" and task_cls.app.main:
                task_name = task_cls.name = '.'.join([task_cls.app.main, name])
            tasks.register(task_cls)
        task = tasks[task_name].__class__

        # decorate with annotations from config.
        task.app.annotate_task(task)
        return task

    def __repr__(cls):
        return "<class Task of %s>" % (cls.app, )


class BaseTask(object):
    """Task base class.

    When called tasks apply the :meth:`run` method.  This method must
    be defined by all tasks (that is unless the :meth:`__call__` method
    is overridden).

    """
    __metaclass__ = TaskType
    __tracer__ = None

    ErrorMail = ErrorMail
    MaxRetriesExceededError = MaxRetriesExceededError

    #: The application instance associated with this task class.
    app = None

    #: Name of the task.
    name = None

    #: If :const:`True` the task is an abstract base class.
    abstract = True

    #: If disabled the worker will not forward magic keyword arguments.
    #: Deprecated and scheduled for removal in v3.0.
    accept_magic_kwargs = False

    #: Request context (set when task is applied).
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

    #: If enabled an email will be sent to :setting:`ADMINS` whenever a task
    #: of this type fails.
    send_error_emails = False
    disable_error_emails = False                            # FIXME

    #: List of exception types to send error emails for.
    error_whitelist = ()

    #: The name of a serializer that are registered with
    #: :mod:`kombu.serialization.registry`.  Default is `"pickle"`.
    serializer = "pickle"

    #: Hard time limit.
    #: Defaults to the :setting:`CELERY_TASK_TIME_LIMIT` setting.
    time_limit = None

    #: Soft time limit.
    #: Defaults to the :setting:`CELERY_TASK_SOFT_TIME_LIMIT` setting.
    soft_time_limit = None

    #: The result store backend used for this task.
    backend = None

    #: If disabled this task won't be registered automatically.
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

    #: When enabled messages for this task will be acknowledged **after**
    #: the task has been executed, and not *just before* which is the
    #: default behavior.
    #:
    #: Please note that this means the task may be executed twice if the
    #: worker crashes mid execution (which may be acceptable for some
    #: applications).
    #:
    #: The application default can be overridden with the
    #: :setting:`CELERY_ACKS_LATE` setting.
    acks_late = False

    #: Default task expiry time.
    expires = None

    #: The type of task *(no longer used)*.
    type = "regular"

    #: Execution strategy used, or the qualified name of one.
    Strategy = "celery.worker.strategy:default"

    def __reduce__(self):
        return (_unpickle_task, (self.name, ), None)

    def run(self, *args, **kwargs):
        """The body of the task executed by workers."""
        raise NotImplementedError("Tasks must define the run method.")

    def start_strategy(self, app, consumer):
        return instantiate(self.Strategy, self, app, consumer)

    @classmethod
    def get_logger(self, loglevel=None, logfile=None, propagate=False,
            **kwargs):
        """Get task-aware logger object."""
        return self.app.log.setup_task_logger(
            loglevel=self.request.loglevel if loglevel is None else loglevel,
            logfile=self.request.logfile if logfile is None else logfile,
            propagate=propagate, task_name=self.name, task_id=self.request.id)

    @classmethod
    def establish_connection(self, connect_timeout=None):
        """Establish a connection to the message broker."""
        return self.app.broker_connection(connect_timeout=connect_timeout)

    @classmethod
    def get_publisher(self, connection=None, exchange=None,
            connect_timeout=None, exchange_type=None, **options):
        """Get a celery task message publisher.

        :rtype :class:`~celery.app.amqp.TaskPublisher`:

        .. warning::

            If you don't specify a connection, one will automatically
            be established for you, in that case you need to close this
            connection after use::

                >>> publisher = self.get_publisher()
                >>> # ... do something with publisher
                >>> publisher.connection.close()

            or used as a context::

                >>> with self.get_publisher() as publisher:
                ...     # ... do something with publisher

        """
        exchange = self.exchange if exchange is None else exchange
        if exchange_type is None:
            exchange_type = self.exchange_type
        connection = connection or self.establish_connection(connect_timeout)
        return self.app.amqp.TaskPublisher(connection=connection,
                                           exchange=exchange,
                                           exchange_type=exchange_type,
                                           routing_key=self.routing_key,
                                           **options)

    @classmethod
    def get_consumer(self, connection=None, connect_timeout=None):
        """Get message consumer.

        :rtype :class:`kombu.messaging.Consumer`:

        .. warning::

            If you don't specify a connection, one will automatically
            be established for you, in that case you need to close this
            connection after use::

                >>> consumer = self.get_consumer()
                >>> # do something with consumer
                >>> consumer.close()
                >>> consumer.connection.close()

        """
        connection = connection or self.establish_connection(connect_timeout)
        return self.app.amqp.TaskConsumer(connection=connection,
                                          exchange=self.exchange,
                                          routing_key=self.routing_key)

    @classmethod
    def delay(self, *args, **kwargs):
        """Star argument version of :meth:`apply_async`.

        Does not support the extra options enabled by :meth:`apply_async`.

        :param \*args: positional arguments passed on to the task.
        :param \*\*kwargs: keyword arguments passed on to the task.

        :returns :class:`celery.result.AsyncResult`:

        """
        return self.apply_async(args, kwargs)

    @classmethod
    def apply_async(self, args=None, kwargs=None,
            task_id=None, publisher=None, connection=None,
            router=None, queues=None, **options):
        """Apply tasks asynchronously by sending a message.

        :keyword args: The positional arguments to pass on to the
                       task (a :class:`list` or :class:`tuple`).

        :keyword kwargs: The keyword arguments to pass on to the
                         task (a :class:`dict`)

        :keyword countdown: Number of seconds into the future that the
                            task should execute. Defaults to immediate
                            execution (do not confuse with the
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
                             of establishing a new one.

        :keyword retry: If enabled sending of the task message will be retried
                        in the event of connection loss or failure.  Default
                        is taken from the :setting:`CELERY_TASK_PUBLISH_RETRY`
                        setting.  Note you need to handle the
                        publisher/connection manually for this to work.

        :keyword retry_policy:  Override the retry policy used.  See the
                                :setting:`CELERY_TASK_PUBLISH_RETRY` setting.

        :keyword routing_key: The routing key used to route the task to a
                              worker server.  Defaults to the
                              :attr:`routing_key` attribute.

        :keyword exchange: The named exchange to send the task to.
                           Defaults to the :attr:`exchange` attribute.

        :keyword exchange_type: The exchange type to initialize the exchange
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
            return self.apply(args, kwargs, task_id=task_id, **options)
        options = dict(extract_exec_options(self), **options)
        options = router.route(options, self.name, args, kwargs)

        publish = publisher or self.app.amqp.publisher_pool.acquire(block=True)
        evd = None
        if conf.CELERY_SEND_TASK_SENT_EVENT:
            evd = self.app.events.Dispatcher(channel=publish.channel,
                                             buffer_while_offline=False)

        try:
            task_id = publish.delay_task(self.name, args, kwargs,
                                         task_id=task_id,
                                         event_dispatcher=evd,
                                         **options)
        finally:
            if not publisher:
                publish.release()

        return self.AsyncResult(task_id)

    @classmethod
    def retry(self, args=None, kwargs=None, exc=None, throw=True,
            eta=None, countdown=None, max_retries=None, **options):
        """Retry the task.

        :param args: Positional arguments to retry with.
        :param kwargs: Keyword arguments to retry with.
        :keyword exc: Optional exception to raise instead of
                      :exc:`~celery.exceptions.MaxRetriesExceededError`
                      when the max restart limit has been exceeded.
        :keyword countdown: Time in seconds to delay the retry for.
        :keyword eta: Explicit time and date to run the retry at
                      (must be a :class:`~datetime.datetime` instance).
        :keyword max_retries: If set, overrides the default retry limit.
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
        max_retries = self.max_retries if max_retries is None else max_retries
        args = request.args if args is None else args
        kwargs = request.kwargs if kwargs is None else kwargs
        delivery_info = request.delivery_info

        # Not in worker or emulated by (apply/always_eager),
        # so just raise the original exception.
        if request.called_directly:
            maybe_reraise()
            raise exc or RetryTaskError("Task can be retried", None)

        if delivery_info:
            options.setdefault("exchange", delivery_info.get("exchange"))
            options.setdefault("routing_key", delivery_info.get("routing_key"))

        if not eta and countdown is None:
            countdown = self.default_retry_delay

        options.update({"retries": request.retries + 1,
                        "task_id": request.id,
                        "countdown": countdown,
                        "eta": eta})

        if max_retries is not None and options["retries"] > max_retries:
            if exc:
                maybe_reraise()
            raise self.MaxRetriesExceededError(
                    "Can't retry %s[%s] args:%s kwargs:%s" % (
                        self.name, options["task_id"], args, kwargs))

        # If task was executed eagerly using apply(),
        # then the retry must also be executed eagerly.
        if request.is_eager:
            return self.apply(args=args, kwargs=kwargs, **options).get()

        self.apply_async(args=args, kwargs=kwargs, **options)
        if throw:
            raise RetryTaskError(
                eta and "Retry at %s" % (eta, )
                     or "Retry in %s secs." % (countdown, ), exc)

    @classmethod
    def apply(self, args=None, kwargs=None, **options):
        """Execute this task locally, by blocking until the task returns.

        :param args: positional arguments passed on to the task.
        :param kwargs: keyword arguments passed on to the task.
        :keyword throw: Re-raise task exceptions.  Defaults to
                        the :setting:`CELERY_EAGER_PROPAGATES_EXCEPTIONS`
                        setting.

        :rtype :class:`celery.result.EagerResult`:

        """
        args = args or []
        kwargs = kwargs or {}
        task_id = options.get("task_id") or uuid()
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

        retval, info = eager_trace_task(task, task_id, args, kwargs,
                                        request=request, propagate=throw)
        if isinstance(retval, ExceptionInfo):
            retval = retval.exception
        state, tb = states.SUCCESS, ''
        if info is not None:
            state, tb = info.state, info.strtb
        return EagerResult(task_id, retval, state, traceback=tb)

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

    def on_retry(self, exc, task_id, args, kwargs, einfo):
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

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
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

    def on_failure(self, exc, task_id, args, kwargs, einfo):
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

    def send_error_email(self, context, exc, **kwargs):
        if self.send_error_emails and not self.disable_error_emails:
            sender = self.ErrorMail(self, **kwargs)
            sender.send(context, exc)

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

    def execute(self, request, pool, loglevel, logfile, **kwargs):
        """The method the worker calls to execute the task.

        :param request: A :class:`~celery.worker.job.Request`.
        :param pool: A task pool.
        :param loglevel: Current loglevel.
        :param logfile: Name of the currently used logfile.

        :keyword consumer: The :class:`~celery.worker.consumer.Consumer`.

        """
        request.execute_using_pool(pool, loglevel, logfile)

    def __repr__(self):
        """`repr(task)`"""
        return "<@task: %s>" % (self.name, )

    @classmethod
    def subtask(cls, *args, **kwargs):
        """Returns :class:`~celery.task.sets.subtask` object for
        this task, wrapping arguments and execution options
        for a single task invocation."""
        from ...task.sets import subtask
        return subtask(cls, *args, **kwargs)

    @property
    def __name__(self):
        return self.__class__.__name__
