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

import os
import warnings
import platform as _platform

from contextlib import contextmanager
from copy import deepcopy
from functools import wraps

from kombu.clocks import LamportClock

from .. import datastructures
from .. import platforms
from ..exceptions import AlwaysEagerIgnored
from ..utils import cached_property, instantiate, lpmerge

from .defaults import DEFAULTS, find_deprecated_settings, find

import kombu
if kombu.VERSION < (2, 0):
    raise ImportError("Celery requires Kombu version 1.1.0 or higher.")

BUGREPORT_INFO = """
platform -> system:%(system)s arch:%(arch)s imp:%(py_i)s
software -> celery:%(celery_v)s kombu:%(kombu_v)s py:%(py_v)s
settings -> transport:%(transport)s results:%(results)s
"""


class Settings(datastructures.ConfigurationView):

    @property
    def CELERY_RESULT_BACKEND(self):
        """Resolves deprecated alias ``CELERY_BACKEND``."""
        return self.get("CELERY_RESULT_BACKEND") or self.get("CELERY_BACKEND")

    @property
    def BROKER_TRANSPORT(self):
        """Resolves compat aliases :setting:`BROKER_BACKEND`
        and :setting:`CARROT_BACKEND`."""
        return (self.get("BROKER_TRANSPORT") or
                self.get("BROKER_BACKEND") or
                self.get("CARROT_BACKEND"))

    @property
    def BROKER_BACKEND(self):
        """Deprecated compat alias to :attr:`BROKER_TRANSPORT`."""
        return self.BROKER_TRANSPORT

    @property
    def BROKER_HOST(self):
        return (os.environ.get("CELERY_BROKER_URL") or
                self.get("BROKER_URL") or
                self.get("BROKER_HOST"))

    def find_option(self, name, namespace="celery"):
        return find(name, namespace)

    def get_by_parts(self, *parts):
        return self["_".join(filter(None, parts))]

    def find_value_for_key(self, name, namespace="celery"):
        ns, key, _ = self.find_option(name, namespace=namespace)
        return self.get_by_parts(ns, key)


class BaseApp(object):
    """Base class for apps."""
    SYSTEM = platforms.SYSTEM
    IS_OSX = platforms.IS_OSX
    IS_WINDOWS = platforms.IS_WINDOWS

    amqp_cls = "celery.app.amqp:AMQP"
    backend_cls = None
    events_cls = "celery.events:Events"
    loader_cls = "celery.loaders.app:AppLoader"
    log_cls = "celery.log:Logging"
    control_cls = "celery.task.control:Control"

    _pool = None

    def __init__(self, main=None, loader=None, backend=None,
            amqp=None, events=None, log=None, control=None,
            set_as_current=True, accept_magic_kwargs=False, **kwargs):
        self.main = main
        self.amqp_cls = amqp or self.amqp_cls
        self.backend_cls = backend or self.backend_cls
        self.events_cls = events or self.events_cls
        self.loader_cls = loader or self.loader_cls
        self.log_cls = log or self.log_cls
        self.control_cls = control or self.control_cls
        self.set_as_current = set_as_current
        self.accept_magic_kwargs = accept_magic_kwargs
        self.clock = LamportClock()

        self.on_init()

    def on_init(self):
        """Called at the end of the constructor."""
        pass

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
        """Read configuration from argv.

        The config

        """
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

    def AsyncResult(self, task_id, backend=None, task_name=None):
        """Create :class:`celery.result.BaseAsyncResult` instance."""
        from ..result import BaseAsyncResult
        return BaseAsyncResult(task_id, app=self, task_name=task_name,
                               backend=backend or self.backend)

    def TaskSetResult(self, taskset_id, results, **kwargs):
        """Create :class:`celery.result.TaskSetResult` instance."""
        from ..result import TaskSetResult
        return TaskSetResult(taskset_id, results, app=self)

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
        find_deprecated_settings(c)
        return c

    def now(self):
        return self.loader.now(utc=self.conf.CELERY_ENABLE_UTC)

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
        if queues:
            return self.amqp.queues.select_subset(queues,
                                    self.conf.CELERY_CREATE_MISSING_QUEUES)

    def either(self, default_key, *values):
        """Fallback to the value of a configuration key if none of the
        `*values` are true."""
        for value in values:
            if value is not None:
                return value
        return self.conf.get(default_key)

    def merge(self, l, r):
        """Like `dict(a, **b)` except it will keep values from `a`
        if the value in `b` is :const:`None`."""
        return lpmerge(l, r)

    def _get_backend(self):
        from ..backends import get_backend_cls
        return get_backend_cls(
                    self.backend_cls or self.conf.CELERY_RESULT_BACKEND,
                    loader=self.loader)(app=self)

    def _get_config(self):
        return Settings({}, [self.prepare_config(self.loader.conf),
                             deepcopy(DEFAULTS)])

    def _after_fork(self, obj_):
        if self._pool:
            self._pool.force_close_all()
            self._pool = None

    def bugreport(self):
        import celery
        import kombu
        return BUGREPORT_INFO % {"system": _platform.system(),
                                 "arch": _platform.architecture(),
                                 "py_i": platforms.pyimplementation(),
                                 "celery_v": celery.__version__,
                                 "kombu_v": kombu.__version__,
                                 "py_v": _platform.python_version(),
                                 "transport": self.conf.BROKER_TRANSPORT,
                                 "results": self.conf.CELERY_RESULT_BACKEND}

    @property
    def pool(self):
        if self._pool is None:
            try:
                from multiprocessing.util import register_after_fork
                register_after_fork(self, self._after_fork)
            except ImportError:
                pass
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
        :class:`~celery.task.control.Control`."""
        return instantiate(self.control_cls, app=self)

    @cached_property
    def events(self):
        """Sending/receiving events.  See :class:`~celery.events.Events`. """
        return instantiate(self.events_cls, app=self)

    @cached_property
    def loader(self):
        """Current loader."""
        from ..loaders import get_loader_cls
        return get_loader_cls(self.loader_cls)(app=self)

    @cached_property
    def log(self):
        """Logging utilities.  See :class:`~celery.log.Logging`."""
        return instantiate(self.log_cls, app=self)

    @cached_property
    def tasks(self):
        from ..registry import tasks
        return tasks
