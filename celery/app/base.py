# -*- coding: utf-8 -*-
"""
    celery.app.base
    ~~~~~~~~~~~~~~~

    Application Base Class.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import warnings

from collections import deque
from contextlib import contextmanager
from copy import deepcopy
from functools import wraps

from kombu.clocks import LamportClock

from celery import platforms
from celery.backends import get_backend_by_url
from celery.exceptions import AlwaysEagerIgnored
from celery.loaders import get_loader_cls
from celery.local import PromiseProxy, maybe_evaluate
from celery.utils import cached_property, register_after_fork
from celery.utils.functional import first
from celery.utils.imports import instantiate, symbol_by_name

from . import annotations
from .builtins import load_builtin_tasks
from .defaults import DEFAULTS, find_deprecated_settings
from .state import _tls
from .utils import AppPickler, Settings, bugreport, _unpickle_app


class App(object):
    """Celery Application.

    :param main: Name of the main module if running as `__main__`.
    :keyword loader: The loader class, or the name of the loader class to use.
                     Default is :class:`celery.loaders.app.AppLoader`.
    :keyword backend: The result store backend class, or the name of the
                      backend class to use. Default is the value of the
                      :setting:`CELERY_RESULT_BACKEND` setting.
    :keyword amqp: AMQP object or class name.
    :keyword events: Events object or class name.
    :keyword log: Log object or class name.
    :keyword control: Control object or class name.
    :keyword set_as_current:  Make this the global current app.

    """
    Pickler = AppPickler

    SYSTEM = platforms.SYSTEM
    IS_OSX, IS_WINDOWS = platforms.IS_OSX, platforms.IS_WINDOWS

    amqp_cls = "celery.app.amqp:AMQP"
    backend_cls = None
    events_cls = "celery.events:Events"
    loader_cls = "celery.loaders.app:AppLoader"
    log_cls = "celery.app.log:Logging"
    control_cls = "celery.app.control:Control"
    registry_cls = "celery.app.registry:TaskRegistry"
    _pool = None

    def __init__(self, main=None, loader=None, backend=None,
            amqp=None, events=None, log=None, control=None,
            set_as_current=True, accept_magic_kwargs=False,
            tasks=None, broker=None, **kwargs):
        self.clock = LamportClock()
        self.main = main
        self.amqp_cls = amqp or self.amqp_cls
        self.backend_cls = backend or self.backend_cls
        self.events_cls = events or self.events_cls
        self.loader_cls = loader or self.loader_cls
        self.log_cls = log or self.log_cls
        self.control_cls = control or self.control_cls
        self.set_as_current = set_as_current
        self.registry_cls = self.registry_cls if tasks is None else tasks
        self.accept_magic_kwargs = accept_magic_kwargs

        self.finalized = False
        self._pending = deque()
        self._tasks = instantiate(self.registry_cls)

        # these options are moved to the config to
        # simplify pickling of the app object.
        self._preconf = {}
        if broker:
            self._preconf["BROKER_URL"] = broker

        if self.set_as_current:
            self.set_current()
        self.on_init()

    def set_current(self):
        """Make this the current app for this thread."""
        _tls.current_app = self

    def on_init(self):
        pass

    def create_task_cls(self):
        """Creates a base task class using default configuration
        taken from this app."""
        return self.subclass_with_self("celery.app.task:BaseTask", name="Task",
                                       attribute="_app", abstract=True)

    def subclass_with_self(self, Class, name=None, attribute="app", **kw):
        """Subclass an app-compatible class by setting its app attribute
        to be this app instance.

        App-compatible means that the class has a class attribute that
        provides the default app it should use, e.g.
        ``class Foo: app = None``.

        :param Class: The app-compatible class to subclass.
        :keyword name: Custom name for the target class.
        :keyword attribute: Name of the attribute holding the app,
                            default is "app".

        """
        Class = symbol_by_name(Class)
        return type(name or Class.__name__, (Class, ), dict({attribute: self,
            "__module__": Class.__module__, "__doc__": Class.__doc__}, **kw))

    @cached_property
    def Worker(self):
        """Create new :class:`~celery.apps.worker.Worker` instance."""
        return self.subclass_with_self("celery.apps.worker:Worker")

    @cached_property
    def WorkController(self, **kwargs):
        return self.subclass_with_self("celery.worker:WorkController")

    @cached_property
    def Beat(self, **kwargs):
        """Create new :class:`~celery.apps.beat.Beat` instance."""
        return self.subclass_with_self("celery.apps.beat:Beat")

    @cached_property
    def TaskSet(self):
        return self.subclass_with_self("celery.task.sets:group")

    def start(self, argv=None):
        """Run :program:`celery` using `argv`.  Uses :data:`sys.argv`
        if `argv` is not specified."""
        return instantiate("celery.bin.celery:CeleryCommand", app=self) \
                    .execute_from_commandline(argv)

    def worker_main(self, argv=None):
        """Run :program:`celeryd` using `argv`.  Uses :data:`sys.argv`
        if `argv` is not specified."""
        return instantiate("celery.bin.celeryd:WorkerCommand", app=self) \
                    .execute_from_commandline(argv)

    def task(self, *args, **options):
        """Decorator to create a task class out of any callable.

        **Examples:**

        .. code-block:: python

            @task
            def refresh_feed(url):
                return ...

        with setting extra options:

        .. code-block:: python

            @task(exchange="feeds")
            def refresh_feed(url):
                return ...

        .. admonition:: App Binding

            For custom apps the task decorator returns proxy
            objects, so that the act of creating the task is not performed
            until the task is used or the task registry is accessed.

            If you are depending on binding to be deferred, then you must
            not access any attributes on the returned object until the
            application is fully set up (finalized).

        """

        def inner_create_task_cls(**options):

            def _create_task_cls(fun):
                if self.accept_magic_kwargs:  # compat mode
                    return self._task_from_fun(fun, **options)

                # return a proxy object that is only evaluated when first used
                promise = PromiseProxy(self._task_from_fun, (fun, ), options)
                self._pending.append(promise)
                return promise

            return _create_task_cls

        if len(args) == 1 and callable(args[0]):
            return inner_create_task_cls(**options)(*args)
        return inner_create_task_cls(**options)

    def _task_from_fun(self, fun, **options):
        base = options.pop("base", None) or self.Task

        T = type(fun.__name__, (base, ), dict({
                "app": self,
                "accept_magic_kwargs": False,
                "run": staticmethod(fun),
                "__doc__": fun.__doc__,
                "__module__": fun.__module__}, **options))()
        task = self._tasks[T.name]  # return global instance.
        task.bind(self)
        return task

    def annotate_task(self, task):
        if self.annotations:
            match = annotations._first_match(self.annotations, task)
            for attr, value in (match or {}).iteritems():
                setattr(task, attr, value)
            match_any = annotations._first_match_any(self.annotations)
            for attr, value in (match_any or {}).iteritems():
                setattr(task, attr, value)

    @cached_property
    def Task(self):
        """Default Task base class for this application."""
        return self.create_task_cls()

    @cached_property
    def annotations(self):
        return annotations.prepare(self.conf.CELERY_ANNOTATIONS)

    def __repr__(self):
        return "<Celery: %s:0x%x>" % (self.main or "__main__", id(self), )

    def __reduce__(self):
        # Reduce only pickles the configuration changes,
        # so the default configuration doesn't have to be passed
        # between processes.
        return (_unpickle_app, (self.__class__, self.Pickler)
                              + self.__reduce_args__())

    def __reduce_args__(self):
        return (self.main,
                self.conf.changes,
                self.loader_cls,
                self.backend_cls,
                self.amqp_cls,
                self.events_cls,
                self.log_cls,
                self.control_cls,
                self.accept_magic_kwargs)

    def finalize(self):
        if not self.finalized:
            load_builtin_tasks(self)

            pending = self._pending
            while pending:
                maybe_evaluate(pending.pop())
            self.finalized = True

    def config_from_object(self, obj, silent=False):
        """Read configuration from object, where object is either
        a object, or the name of a module to import.

            >>> celery.config_from_object("myapp.celeryconfig")

            >>> from myapp import celeryconfig
            >>> celery.config_from_object(celeryconfig)

        """
        del(self.conf)
        return self.loader.config_from_object(obj, silent=silent)

    def config_from_envvar(self, variable_name, silent=False):
        """Read configuration from environment variable.

        The value of the environment variable must be the name
        of a module to import.

            >>> os.environ["CELERY_CONFIG_MODULE"] = "myapp.celeryconfig"
            >>> celery.config_from_envvar("CELERY_CONFIG_MODULE")

        """
        del(self.conf)
        return self.loader.config_from_envvar(variable_name, silent=silent)

    def config_from_cmdline(self, argv, namespace="celery"):
        """Read configuration from argv."""
        self.conf.update(self.loader.cmdline_config_parser(argv, namespace))

    def send_task(self, name, args=None, kwargs=None, countdown=None,
            eta=None, task_id=None, publisher=None, connection=None,
            connect_timeout=None, result_cls=None, expires=None,
            queues=None, **options):
        """Send task by name.

        :param name: Name of task to execute (e.g. `"tasks.add"`).
        :keyword result_cls: Specify custom result class. Default is
            using :meth:`AsyncResult`.

        Supports the same arguments as
        :meth:`~celery.app.task.BaseTask.apply_async`.

        """
        if self.conf.CELERY_ALWAYS_EAGER:
            warnings.warn(AlwaysEagerIgnored(
                "CELERY_ALWAYS_EAGER has no effect on send_task"))

        router = self.amqp.Router(queues)
        result_cls = result_cls or self.AsyncResult

        options.setdefault("compression",
                           self.conf.CELERY_MESSAGE_COMPRESSION)
        options = router.route(options, name, args, kwargs)
        exchange = options.get("exchange")
        exchange_type = options.get("exchange_type")

        with self.default_connection(connection, connect_timeout) as conn:
            publish = publisher or self.amqp.TaskPublisher(conn,
                                            exchange=exchange,
                                            exchange_type=exchange_type)
            try:
                new_id = publish.delay_task(name, args, kwargs,
                                            task_id=task_id,
                                            countdown=countdown, eta=eta,
                                            expires=expires, **options)
            finally:
                publisher or publish.close()
            return result_cls(new_id)

    @cached_property
    def AsyncResult(self):
        return self.subclass_with_self("celery.result:AsyncResult")

    @cached_property
    def TaskSetResult(self):
        return self.subclass_with_self("celery.result:TaskSetResult")

    def broker_connection(self, hostname=None, userid=None,
            password=None, virtual_host=None, port=None, ssl=None,
            insist=None, connect_timeout=None, transport=None,
            transport_options=None, **kwargs):
        """Establish a connection to the message broker.

        :keyword hostname: defaults to the :setting:`BROKER_HOST` setting.
        :keyword userid: defaults to the :setting:`BROKER_USER` setting.
        :keyword password: defaults to the :setting:`BROKER_PASSWORD` setting.
        :keyword virtual_host: defaults to the :setting:`BROKER_VHOST` setting.
        :keyword port: defaults to the :setting:`BROKER_PORT` setting.
        :keyword ssl: defaults to the :setting:`BROKER_USE_SSL` setting.
        :keyword insist: defaults to the :setting:`BROKER_INSIST` setting.
        :keyword connect_timeout: defaults to the
            :setting:`BROKER_CONNECTION_TIMEOUT` setting.
        :keyword backend_cls: defaults to the :setting:`BROKER_TRANSPORT`
            setting.

        :returns :class:`kombu.connection.BrokerConnection`:

        """
        conf = self.conf
        return self.amqp.BrokerConnection(
                    hostname or conf.BROKER_HOST,
                    userid or conf.BROKER_USER,
                    password or conf.BROKER_PASSWORD,
                    virtual_host or conf.BROKER_VHOST,
                    port or conf.BROKER_PORT,
                    transport=transport or conf.BROKER_TRANSPORT,
                    insist=self.either("BROKER_INSIST", insist),
                    ssl=self.either("BROKER_USE_SSL", ssl),
                    connect_timeout=self.either(
                                "BROKER_CONNECTION_TIMEOUT", connect_timeout),
                    transport_options=dict(conf.BROKER_TRANSPORT_OPTIONS,
                                           **transport_options or {}))

    @contextmanager
    def default_connection(self, connection=None, connect_timeout=None):
        """For use within a with-statement to get a connection from the pool
        if one is not already provided.

        :keyword connection: If not provided, then a connection will be
                             acquired from the connection pool.
        :keyword connect_timeout: *No longer used.*

        """
        if connection:
            yield connection
        else:
            with self.pool.acquire(block=True) as connection:
                yield connection

    def with_default_connection(self, fun):
        """With any function accepting `connection` and `connect_timeout`
        keyword arguments, establishes a default connection if one is
        not already passed to it.

        Any automatically established connection will be closed after
        the function returns.

        **Deprecated**

        Use ``with app.default_connection(connection)`` instead.

        """
        @wraps(fun)
        def _inner(*args, **kwargs):
            connection = kwargs.pop("connection", None)
            with self.default_connection(connection) as c:
                return fun(*args, **dict(kwargs, connection=c))
        return _inner

    def prepare_config(self, c):
        """Prepare configuration before it is merged with the defaults."""
        if self._preconf:
            for key, value in self._preconf.iteritems():
                setattr(c, key, value)
        return find_deprecated_settings(c)

    def now(self):
        return self.loader.now()

    def mail_admins(self, subject, body, fail_silently=False):
        """Send an email to the admins in the :setting:`ADMINS` setting."""
        if self.conf.ADMINS:
            to = [admin_email for _, admin_email in self.conf.ADMINS]
            return self.loader.mail_admins(subject, body, fail_silently, to=to,
                                       sender=self.conf.SERVER_EMAIL,
                                       host=self.conf.EMAIL_HOST,
                                       port=self.conf.EMAIL_PORT,
                                       user=self.conf.EMAIL_HOST_USER,
                                       password=self.conf.EMAIL_HOST_PASSWORD,
                                       timeout=self.conf.EMAIL_TIMEOUT,
                                       use_ssl=self.conf.EMAIL_USE_SSL,
                                       use_tls=self.conf.EMAIL_USE_TLS)

    def select_queues(self, queues=None):
        return self.amqp.queues.select_subset(queues,
                                self.conf.CELERY_CREATE_MISSING_QUEUES)

    def either(self, default_key, *values):
        """Fallback to the value of a configuration key if none of the
        `*values` are true."""
        return first(None, values) or self.conf.get(default_key)

    def bugreport(self):
        return bugreport(self)

    def _get_backend(self):
        backend, url = get_backend_by_url(
                self.backend_cls or self.conf.CELERY_RESULT_BACKEND,
                self.loader)
        return backend(app=self, url=url)

    def _get_config(self):
        return Settings({}, [self.prepare_config(self.loader.conf),
                             deepcopy(DEFAULTS)])

    def _after_fork(self, obj_):
        if self._pool:
            self._pool.force_close_all()
            self._pool = None

    @property
    def pool(self):
        if self._pool is None:
            register_after_fork(self, self._after_fork)
            self._pool = self.broker_connection().Pool(
                            limit=self.conf.BROKER_POOL_LIMIT)
        return self._pool

    @cached_property
    def amqp(self):
        """Sending/receiving messages.  See :class:`~celery.app.amqp.AMQP`."""
        return instantiate(self.amqp_cls, app=self)

    @cached_property
    def backend(self):
        """Storing/retrieving task state.  See
        :class:`~celery.backend.base.BaseBackend`."""
        return self._get_backend()

    @cached_property
    def conf(self):
        """Current configuration (dict and attribute access)."""
        return self._get_config()

    @cached_property
    def control(self):
        """Controlling worker nodes.  See
        :class:`~celery.app.control.Control`."""
        return instantiate(self.control_cls, app=self)

    @cached_property
    def events(self):
        """Sending/receiving events.  See :class:`~celery.events.Events`. """
        return instantiate(self.events_cls, app=self)

    @cached_property
    def loader(self):
        """Current loader."""
        return get_loader_cls(self.loader_cls)(app=self)

    @cached_property
    def log(self):
        """Logging utilities.  See :class:`~celery.log.Logging`."""
        return instantiate(self.log_cls, app=self)

    @cached_property
    def tasks(self):
        """Registry of available tasks.

        Accessing this attribute will also finalize the app.

        """
        self.finalize()
        return self._tasks
