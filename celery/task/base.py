# -*- coding: utf-8 -*-
"""Deprecated task base class.

The task implementation has been moved to :mod:`celery.app.task`.

This contains the backward compatible Task class used in the old API,
and shouldn't be used in new applications.
"""
from __future__ import absolute_import, unicode_literals
from kombu import Exchange
from celery import current_app
from celery.app.task import Context, Task as BaseTask, _reprtask
from celery.five import python_2_unicode_compatible, with_metaclass
from celery.local import Proxy, class_property, reclassmethod
from celery.schedules import maybe_schedule
from celery.utils.log import get_task_logger

__all__ = ['Context', 'Task', 'TaskType', 'PeriodicTask', 'task']

#: list of methods that must be classmethods in the old API.
_COMPAT_CLASSMETHODS = (
    'delay', 'apply_async', 'retry', 'apply', 'subtask_from_request',
    'signature_from_request', 'signature',
    'AsyncResult', 'subtask', '_get_request', '_get_exec_options',
)


@python_2_unicode_compatible
class _CompatShared(object):

    def __init__(self, name, cons):
        self.name = name
        self.cons = cons

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return '<OldTask: %r>' % (self.name,)

    def __call__(self, app):
        return self.cons(app)


class TaskType(type):
    """Meta class for tasks.

    Automatically registers the task in the task registry (except
    if the :attr:`Task.abstract`` attribute is set).

    If no :attr:`Task.name` attribute is provided, then the name is generated
    from the module and class name.
    """

    _creation_count = {}  # used by old non-abstract task classes

    def __new__(cls, name, bases, attrs):
        new = super(TaskType, cls).__new__
        task_module = attrs.get('__module__') or '__main__'

        # - Abstract class: abstract attribute shouldn't be inherited.
        abstract = attrs.pop('abstract', None)
        if abstract or not attrs.get('autoregister', True):
            return new(cls, name, bases, attrs)

        # The 'app' attribute is now a property, with the real app located
        # in the '_app' attribute.  Previously this was a regular attribute,
        # so we should support classes defining it.
        app = attrs.pop('_app', None) or attrs.pop('app', None)

        # Attempt to inherit app from one the bases
        if not isinstance(app, Proxy) and app is None:
            for base in bases:
                if getattr(base, '_app', None):
                    app = base._app
                    break
            else:
                app = current_app._get_current_object()
        attrs['_app'] = app

        # - Automatically generate missing/empty name.
        task_name = attrs.get('name')
        if not task_name:
            attrs['name'] = task_name = app.gen_task_name(name, task_module)

        if not attrs.get('_decorated'):
            # non decorated tasks must also be shared in case
            # an app is created multiple times due to modules
            # imported under multiple names.
            # Hairy stuff,  here to be compatible with 2.x.
            # People shouldn't use non-abstract task classes anymore,
            # use the task decorator.
            from celery._state import connect_on_app_finalize
            unique_name = '.'.join([task_module, name])
            if unique_name not in cls._creation_count:
                # the creation count is used as a safety
                # so that the same task isn't added recursively
                # to the set of constructors.
                cls._creation_count[unique_name] = 1
                connect_on_app_finalize(_CompatShared(
                    unique_name,
                    lambda app: TaskType.__new__(cls, name, bases,
                                                 dict(attrs, _app=app)),
                ))

        # - Create and register class.
        # Because of the way import happens (recursively)
        # we may or may not be the first time the task tries to register
        # with the framework.  There should only be one class for each task
        # name, so we always return the registered version.
        tasks = app._tasks
        if task_name not in tasks:
            tasks.register(new(cls, name, bases, attrs))
        instance = tasks[task_name]
        instance.bind(app)
        return instance.__class__

    def __repr__(self):
        return _reprtask(self)


@with_metaclass(TaskType)
@python_2_unicode_compatible
class Task(BaseTask):
    """Deprecated Task base class.

    Modern applications should use :class:`celery.Task` instead.
    """

    abstract = True
    __bound__ = False
    __v2_compat__ = True

    # - Deprecated compat. attributes -:

    queue = None
    routing_key = None
    exchange = None
    exchange_type = None
    delivery_mode = None
    mandatory = False  # XXX deprecated
    immediate = False  # XXX deprecated
    priority = None
    type = 'regular'

    from_config = BaseTask.from_config + (
        ('exchange_type', 'task_default_exchange_type'),
        ('delivery_mode', 'task_default_delivery_mode'),
    )

    # In old Celery the @task decorator didn't exist, so one would create
    # classes instead and use them directly (e.g., MyTask.apply_async()).
    # the use of classmethods was a hack so that it was not necessary
    # to instantiate the class before using it, but it has only
    # given us pain (like all magic).
    for name in _COMPAT_CLASSMETHODS:
        locals()[name] = reclassmethod(getattr(BaseTask, name))

    @class_property
    def request(self):
        return self._get_request()

    @class_property
    def backend(self):
        if self._backend is None:
            return self.app.backend
        return self._backend

    @backend.setter
    def backend(cls, value):  # noqa
        cls._backend = value

    @classmethod
    def get_logger(cls, **kwargs):
        return get_task_logger(cls.name)

    @classmethod
    def establish_connection(cls):
        """Deprecated method used to get a broker connection.

        Should be replaced with :meth:`@Celery.connection`
        instead, or by acquiring connections from the connection pool:

        Examples:
            >>> # using the connection pool
            >>> with celery.pool.acquire(block=True) as conn:
            ...     pass

            >>> # establish fresh connection
            >>> with celery.connection_for_write() as conn:
            ...     pass
        """
        return cls._get_app().connection_for_write()

    def get_publisher(self, connection=None, exchange=None,
                      exchange_type=None, **options):
        """Deprecated method to get the task publisher (now called producer).

        Should be replaced with :class:`kombu.Producer`:

        .. code-block:: python

            with app.connection_for_write() as conn:
                with app.amqp.Producer(conn) as prod:
                    my_task.apply_async(producer=prod)

            or even better is to use the :class:`@amqp.producer_pool`:

            .. code-block:: python

                with app.producer_or_acquire() as prod:
                    my_task.apply_async(producer=prod)
        """
        exchange = self.exchange if exchange is None else exchange
        if exchange_type is None:
            exchange_type = self.exchange_type
        connection = connection or self.establish_connection()
        return self._get_app().amqp.Producer(
            connection,
            exchange=exchange and Exchange(exchange, exchange_type),
            routing_key=self.routing_key, auto_declare=False, **options)

    @classmethod
    def get_consumer(cls, connection=None, queues=None, **kwargs):
        """Get consumer for the queue this task is sent to.

        Deprecated!

        Should be replaced by :class:`@amqp.TaskConsumer`.
        """
        Q = cls._get_app().amqp
        connection = connection or cls.establish_connection()
        if queues is None:
            queues = Q.queues[cls.queue] if cls.queue else Q.default_queue
        return Q.TaskConsumer(connection, queues, **kwargs)


class PeriodicTask(Task):
    """A task that adds itself to the :setting:`beat_schedule` setting."""

    abstract = True
    ignore_result = True
    relative = False
    options = None
    compat = True

    def __init__(self):
        if not hasattr(self, 'run_every'):
            raise NotImplementedError(
                'Periodic tasks must have a run_every attribute')
        self.run_every = maybe_schedule(self.run_every, self.relative)
        super(PeriodicTask, self).__init__()

    @classmethod
    def on_bound(cls, app):
        app.conf.beat_schedule[cls.name] = {
            'task': cls.name,
            'schedule': cls.run_every,
            'args': (),
            'kwargs': {},
            'options': cls.options or {},
            'relative': cls.relative,
        }


def task(*args, **kwargs):
    """Deprecated decorator, please use :func:`celery.task`."""
    return current_app.task(*args, **dict({'base': Task}, **kwargs))


def periodic_task(*args, **options):
    """Deprecated decorator, please use :setting:`beat_schedule`."""
    return task(**dict({'base': PeriodicTask}, **options))
