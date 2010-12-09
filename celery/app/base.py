"""
celery.app.base
===============

Application Base Class.

:copyright: (c) 2009 - 2010 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
import sys
import platform as _platform

from datetime import timedelta

from celery import routes
from celery.app.defaults import DEFAULTS
from celery.datastructures import ConfigurationView
from celery.utils import noop, isatty, cached_property
from celery.utils.functional import wraps


class BaseApp(object):
    """Base class for apps."""
    SYSTEM = _platform.system()
    IS_OSX = SYSTEM == "Darwin"
    IS_WINDOWS = SYSTEM == "Windows"

    def __init__(self, main=None, loader=None, backend=None,
            set_as_current=True):
        self.main = main
        self.loader_cls = loader or "app"
        self.backend_cls = backend
        self.set_as_current = set_as_current
        self.on_init()

    def on_init(self):
        """Called at the end of the constructor."""
        pass

    def config_from_object(self, obj, silent=False):
        """Read configuration from object, where object is either
        a real object, or the name of an object to import.

            >>> celery.config_from_object("myapp.celeryconfig")

            >>> from myapp import celeryconfig
            >>> celery.config_from_object(celeryconfig)

        """
        del(self.conf)
        return self.loader.config_from_object(obj, silent=silent)

    def config_from_envvar(self, variable_name, silent=False):
        """Read configuration from environment variable.

        The value of the environment variable must be the name
        of an object to import.

            >>> os.environ["CELERY_CONFIG_MODULE"] = "myapp.celeryconfig"
            >>> celery.config_from_envvar("CELERY_CONFIG_MODULE")

        """
        del(self.conf)
        return self.loader.config_from_envvar(variable_name, silent=silent)

    def config_from_cmdline(self, argv, namespace="celery"):
        """Read configuration from argv.

        The config

        """
        config = self.loader.cmdline_config_parser(argv, namespace)
        for key, value in config.items():
            self.conf[key] = value

    def send_task(self, name, args=None, kwargs=None, countdown=None,
            eta=None, task_id=None, publisher=None, connection=None,
            connect_timeout=None, result_cls=None, expires=None,
            queues=None, **options):
        """Send task by name.

        :param name: Name of task to execute (e.g. `"tasks.add"`).
        :keyword result_cls: Specify custom result class. Default is
            using :meth:`AsyncResult`.

        Supports the same arguments as
        :meth:`~celery.task.base.BaseTask.apply_async`.

        """
        router = self.amqp.Router(queues)
        result_cls = result_cls or self.AsyncResult

        options.setdefault("compression",
                           self.conf.CELERY_MESSAGE_COMPRESSION)
        options = router.route(options, name, args, kwargs)
        exchange = options.get("exchange")
        exchange_type = options.get("exchange_type")

        def _do_publish(connection=None, **_):
            publish = publisher or self.amqp.TaskPublisher(connection,
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

        return self.with_default_connection(_do_publish)(
                connection=connection, connect_timeout=connect_timeout)

    def AsyncResult(self, task_id, backend=None, task_name=None):
        """Create :class:`celery.result.BaseAsyncResult` instance."""
        from celery.result import BaseAsyncResult
        return BaseAsyncResult(task_id, app=self,
                               task_name=task_name,
                               backend=backend or self.backend)

    def TaskSetResult(self, taskset_id, results, **kwargs):
        """Create :class:`celery.result.TaskSetResult` instance."""
        from celery.result import TaskSetResult
        return TaskSetResult(taskset_id, results, app=self)

    def broker_connection(self, hostname=None, userid=None,
            password=None, virtual_host=None, port=None, ssl=None,
            insist=None, connect_timeout=None, transport=None, **kwargs):
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
        :keyword backend_cls: defaults to the :setting:`BROKER_BACKEND`
            setting.

        :returns :class:`kombu.connection.BrokerConnection`:

        """
        return self.amqp.BrokerConnection(
                    hostname or self.conf.BROKER_HOST,
                    userid or self.conf.BROKER_USER,
                    password or self.conf.BROKER_PASSWORD,
                    virtual_host or self.conf.BROKER_VHOST,
                    port or self.conf.BROKER_PORT,
                    transport=transport or self.conf.BROKER_BACKEND,
                    insist=self.either("BROKER_INSIST", insist),
                    ssl=self.either("BROKER_USE_SSL", ssl),
                    connect_timeout=self.either(
                                "BROKER_CONNECTION_TIMEOUT", connect_timeout))

    def with_default_connection(self, fun):
        """With any function accepting `connection` and `connect_timeout`
        keyword arguments, establishes a default connection if one is
        not already passed to it.

        Any automatically established connection will be closed after
        the function returns.

        """

        @wraps(fun)
        def _inner(*args, **kwargs):
            connection = kwargs.get("connection")
            timeout = kwargs.get("connect_timeout")
            kwargs["connection"] = conn = connection or \
                    self.broker_connection(connect_timeout=timeout)
            close_connection = not connection and conn.close or noop

            try:
                return fun(*args, **kwargs)
            finally:
                close_connection()
        return _inner

    def pre_config_merge(self, c):
        """Prepare configuration before it is merged with the defaults."""
        if not c.get("CELERY_RESULT_BACKEND"):
            rbackend = c.get("CELERY_BACKEND")
            if rbackend:
                c["CELERY_RESULT_BACKEND"] = rbackend
        if not c.get("BROKER_BACKEND"):
            cbackend = c.get("BROKER_TRANSPORT") or c.get("CARROT_BACKEND")
            if cbackend:
                c["BROKER_BACKEND"] = cbackend
        return c

    def post_config_merge(self, c):
        """Prepare configuration after it has been merged with the
        defaults."""
        if not c.get("CELERY_QUEUES"):
            c["CELERY_QUEUES"] = {
                c.CELERY_DEFAULT_QUEUE: {
                    "exchange": c.CELERY_DEFAULT_EXCHANGE,
                    "exchange_type": c.CELERY_DEFAULT_EXCHANGE_TYPE,
                    "binding_key": c.CELERY_DEFAULT_ROUTING_KEY}}
        c["CELERY_ROUTES"] = routes.prepare(c.get("CELERY_ROUTES") or {})
        if c.get("CELERYD_LOG_COLOR") is None:
            c["CELERYD_LOG_COLOR"] = not c.CELERYD_LOG_FILE and \
                                        isatty(sys.stderr)
            if self.IS_WINDOWS:  # windows console doesn't support ANSI colors
                c["CELERYD_LOG_COLOR"] = False
        if isinstance(c.CELERY_TASK_RESULT_EXPIRES, int):
            c["CELERY_TASK_RESULT_EXPIRES"] = timedelta(
                    seconds=c.CELERY_TASK_RESULT_EXPIRES)

        # Install backend cleanup periodic task.
        if c.CELERY_TASK_RESULT_EXPIRES:
            from celery.schedules import crontab
            c.CELERYBEAT_SCHEDULE.setdefault("celery.backend_cleanup",
                    dict(task="celery.backend_cleanup",
                         schedule=crontab(minute="00", hour="04",
                                          day_of_week="*"),
                         options={"expires": 12 * 3600}))

        return c

    def mail_admins(self, subject, body, fail_silently=False):
        """Send an e-mail to the admins in conf.ADMINS."""
        if not self.conf.ADMINS:
            return
        to = [admin_email for _, admin_email in self.conf.ADMINS]
        self.loader.mail_admins(subject, body, fail_silently,
                                to=to, sender=self.conf.SERVER_EMAIL,
                                host=self.conf.EMAIL_HOST,
                                port=self.conf.EMAIL_PORT,
                                user=self.conf.EMAIL_HOST_USER,
                                password=self.conf.EMAIL_HOST_PASSWORD,
                                timeout=self.conf.EMAIL_TIMEOUT)

    def either(self, default_key, *values):
        """Fallback to the value of a configuration key if none of the
        `*values` are true."""
        for value in values:
            if value is not None:
                return value
        return self.conf.get(default_key)

    def merge(self, a, b):
        """Like `dict(a, **b)` except it will keep values from `a`
        if the value in `b` is :const:`None`."""
        b = dict(b)
        for key, value in a.items():
            if b.get(key) is None:
                b[key] = value
        return b

    def _get_backend(self):
        from celery.backends import get_backend_cls
        backend_cls = self.backend_cls or self.conf.CELERY_RESULT_BACKEND
        backend_cls = get_backend_cls(backend_cls, loader=self.loader)
        return backend_cls(app=self)

    def _get_config(self):
        return self.post_config_merge(ConfigurationView(
                    self.pre_config_merge(self.loader.conf), DEFAULTS))

    @cached_property
    def amqp(self):
        """Sending/receiving messages.

        See :class:`~celery.app.amqp.AMQP`.

        """
        from celery.app.amqp import AMQP
        return AMQP(self)

    @cached_property
    def backend(self):
        """Storing/retreiving task state.

        See :class:`~celery.backend.base.BaseBackend`.

        """
        return self._get_backend()

    @cached_property
    def loader(self):
        """Current loader."""
        from celery.loaders import get_loader_cls
        return get_loader_cls(self.loader_cls)(app=self)

    @cached_property
    def conf(self):
        """Current configuration (dict and attribute access)."""
        return self._get_config()

    @cached_property
    def control(self):
        """Controlling worker nodes.

        See :class:`~celery.task.control.Control`.

        """
        from celery.task.control import Control
        return Control(app=self)

    @cached_property
    def log(self):
        """Logging utilities.

        See :class:`~celery.log.Logging`.

        """
        from celery.log import Logging
        return Logging(app=self)

    @cached_property
    def events(self):
        """Sending/receiving events.

        See :class:`~celery.events.Events`.

        """
        from celery.events import Events
        return Events(app=self)
