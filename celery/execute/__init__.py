from celery import conf
from celery.datastructures import ExceptionInfo
from celery.execute.trace import TaskTrace
from celery.messaging import with_connection
from celery.messaging import TaskPublisher
from celery.registry import tasks
from celery.result import AsyncResult, EagerResult
from celery.routes import route
from celery.utils import gen_unique_id, fun_takes_kwargs, mattrgetter

extract_exec_options = mattrgetter("queue", "routing_key", "exchange",
                                   "immediate", "mandatory",
                                   "priority", "serializer",
                                   "delivery_mode")


@with_connection
def apply_async(task, args=None, kwargs=None, countdown=None, eta=None,
        task_id=None, publisher=None, connection=None, connect_timeout=None,
        routes=None, routing_table=None, **options):
    """Run a task asynchronously by the celery daemon(s).

    :param task: The :class:`~celery.task.base.Task` to run.

    :keyword args: The positional arguments to pass on to the
      task (a :class:`list` or :class:`tuple`).

    :keyword kwargs: The keyword arguments to pass on to the
      task (a :class:`dict`)

    :keyword countdown: Number of seconds into the future that the task should
      execute. Defaults to immediate delivery (Do not confuse that with
      the ``immediate`` setting, they are unrelated).

    :keyword eta: A :class:`~datetime.datetime` object that describes the
      absolute time when the task should execute. May not be specified
      if ``countdown`` is also supplied. (Do not confuse this with the
      ``immediate`` setting, they are unrelated).

    :keyword connection: Re-use existing broker connection instead
      of establishing a new one. The ``connect_timeout`` argument is
      not respected if this is set.

    :keyword connect_timeout: The timeout in seconds, before we give up
      on establishing a connection to the AMQP server.

    :keyword routing_key: The routing key used to route the task to a worker
      server. Defaults to the tasks :attr:`~celery.task.base.Task.exchange`
      attribute.

    :keyword exchange: The named exchange to send the task to. Defaults to
      the tasks :attr:`~celery.task.base.Task.exchange` attribute.

    :keyword exchange_type: The exchange type to initalize the exchange as
      if not already declared. Defaults to the tasks
      :attr:`~celery.task.base.Task.exchange_type` attribute.

    :keyword immediate: Request immediate delivery. Will raise an exception
      if the task cannot be routed to a worker immediately.
      (Do not confuse this parameter with the ``countdown`` and ``eta``
      settings, as they are unrelated). Defaults to the tasks
      :attr:`~celery.task.base.Task.immediate` attribute.

    :keyword mandatory: Mandatory routing. Raises an exception if there's
      no running workers able to take on this task. Defaults to the tasks
      :attr:`~celery.task.base.Task.mandatory` attribute.

    :keyword priority: The task priority, a number between ``0`` and ``9``.
      Defaults to the tasks :attr:`~celery.task.base.Task.priority` attribute.

    :keyword serializer: A string identifying the default serialization
      method to use. Defaults to the ``CELERY_TASK_SERIALIZER`` setting.
      Can be ``pickle`` ``json``, ``yaml``, or any custom serialization
      methods that have been registered with
      :mod:`carrot.serialization.registry`. Defaults to the tasks
      :attr:`~celery.task.base.Task.serializer` attribute.

    **Note**: If the ``CELERY_ALWAYS_EAGER`` setting is set, it will be
    replaced by a local :func:`apply` call instead.

    """
    if routes is None:
        routes = conf.ROUTES
    if routing_table is None:
        routing_table = conf.get_routing_table()

    if conf.ALWAYS_EAGER:
        return apply(task, args, kwargs, task_id=task_id)

    task = tasks[task.name] # get instance from registry

    options = dict(extract_exec_options(task), **options)
    options = route(routes, options, routing_table,
                    task.name, args, kwargs)
    exchange = options.get("exchange")
    exchange_type = options.get("exchange_type")

    publish = publisher or task.get_publisher(connection, exchange=exchange,
                                              exchange_type=exchange_type)
    try:
        task_id = publish.delay_task(task.name, args, kwargs, task_id=task_id,
                                     countdown=countdown, eta=eta, **options)
    finally:
        publisher or publish.close()

    return task.AsyncResult(task_id)


@with_connection
def send_task(name, args=None, kwargs=None, countdown=None, eta=None,
        task_id=None, publisher=None, connection=None, connect_timeout=None,
        result_cls=AsyncResult, **options):
    """Send task by name.

    Useful if you don't have access to the :class:`~celery.task.base.Task`
    class.

    :param name: Name of task to execute.

    Supports the same arguments as :func:`apply_async`.

    """
    exchange = options.get("exchange")
    exchange_type = options.get("exchange_type")

    publish = publisher or TaskPublisher(connection, exchange=exchange,
                                         exchange_type=exchange_type)
    try:
        task_id = publish.delay_task(name, args, kwargs, task_id=task_id,
                                     countdown=countdown, eta=eta, **options)
    finally:
        publisher or publish.close()

    return result_cls(task_id)


def delay_task(task_name, *args, **kwargs):
    """Delay a task for execution by the ``celery`` daemon.

    :param task_name: the name of a task registered in the task registry.
    :param \*args: positional arguments to pass on to the task.
    :param \*\*kwargs: keyword arguments to pass on to the task.

    :raises celery.exceptions.NotRegistered: exception if no such task
        has been registered in the task registry.

    :returns: :class:`celery.result.AsyncResult`.

    Example

        >>> r = delay_task("update_record", name="George Costanza", age=32)
        >>> r.ready()
        True
        >>> r.result
        "Record was updated"

    """
    return apply_async(tasks[task_name], args, kwargs)


def apply(task, args, kwargs, **options):
    """Apply the task locally.

    :keyword throw: Re-raise task exceptions. Defaults to
        the ``CELERY_EAGER_PROPAGATES_EXCEPTIONS`` setting.

    This will block until the task completes, and returns a
    :class:`celery.result.EagerResult` instance.

    """
    args = args or []
    kwargs = kwargs or {}
    task_id = options.get("task_id", gen_unique_id())
    retries = options.get("retries", 0)
    throw = options.pop("throw", conf.EAGER_PROPAGATES_EXCEPTIONS)

    task = tasks[task.name] # Make sure we get the instance, not class.

    default_kwargs = {"task_name": task.name,
                      "task_id": task_id,
                      "task_retries": retries,
                      "task_is_eager": True,
                      "logfile": options.get("logfile"),
                      "delivery_info": {"is_eager": True},
                      "loglevel": options.get("loglevel", 0)}
    supported_keys = fun_takes_kwargs(task.run, default_kwargs)
    extend_with = dict((key, val) for key, val in default_kwargs.items()
                            if key in supported_keys)
    kwargs.update(extend_with)

    trace = TaskTrace(task.name, task_id, args, kwargs, task=task)
    retval = trace.execute()
    if isinstance(retval, ExceptionInfo):
        if throw:
            raise retval.exception
        retval = retval.exception
    return EagerResult(task_id, retval, trace.status, traceback=trace.strtb)
