"""
celery.app.base
===============

Application Base Class.

:copyright: (c) 2009 - 2011 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
import platform as _platform

from copy import deepcopy
from threading import Lock

from kombu.utils import cached_property

from celery.app.defaults import DEFAULTS
from celery.datastructures import ConfigurationView
from celery.utils import instantiate, lpmerge
from celery.utils.functional import wraps


class LamportClock(object):
    """Lamports logical clock.

    From Wikipedia:

    "A Lamport logical clock is a monotonically incrementing software counter
    maintained in each process.  It follows some simple rules:

        * A process increments its counter before each event in that process;
        * When a process sends a message, it includes its counter value with
          the message;
        * On receiving a message, the receiver process sets its counter to be
          greater than the maximum of its own value and the received value
          before it considers the message received.

    Conceptually, this logical clock can be thought of as a clock that only
    has meaning in relation to messages moving between processes.  When a
    process receives a message, it resynchronizes its logical clock with
    the sender.

    .. seealso::

        http://en.wikipedia.org/wiki/Lamport_timestamps
        http://en.wikipedia.org/wiki/Lamport's_Distributed_
            Mutual_Exclusion_Algorithm

    *Usage*

    When sending a message use :meth:`forward` to increment the clock,
    when receiving a message use :meth:`adjust` to sync with
    the timestamp of the incoming message.

    """
    #: The clocks current value.
    value = 0

    def __init__(self, initial_value=0):
        self.value = initial_value
        self._mutex = Lock()

    def adjust(self, other):
        self._mutex.acquire()
        try:
            self.value = max(self.value, other) + 1
        finally:
            self._mutex.release()

    def forward(self):
        self._mutex.acquire()
        try:
            self.value += 1
        finally:
            self._mutex.release()
        return self.value


class BaseApp(object):
    """Base class for apps."""
    SYSTEM = _platform.system()
    IS_OSX = SYSTEM == "Darwin"
    IS_WINDOWS = SYSTEM == "Windows"

    amqp_cls = "celery.app.amqp.AMQP"
    backend_cls = None
    events_cls = "celery.events.Events"
    loader_cls = "app"
    log_cls = "celery.log.Logging"
    control_cls = "celery.task.control.Control"

    def __init__(self, main=None, loader=None, backend=None,
            amqp=None, events=None, log=None, control=None,
            set_as_current=True, accept_magic_kwargs=False):
        self.main = main
        self.amqp_cls = amqp or self.amqp_cls
        self.backend_cls = backend or self.backend_cls
        self.events_cls = events or self.events_cls
        self.loader_cls = loader or self.loader_cls
        self.log_cls = log or self.log_cls
        self.control_cls = control or self.control_cls
        self.set_as_current = set_as_current
        self.accept_magic_kwargs = accept_magic_kwargs
        self.on_init()
        self.clock = LamportClock()

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
                                "BROKER_CONNECTION_TIMEOUT", connect_timeout),
                    transport_options=self.conf.BROKER_TRANSPORT_OPTIONS)

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
            close_connection = not connection and conn.close or None

            try:
                return fun(*args, **kwargs)
            finally:
                if close_connection:
                    close_connection()
        return _inner

    def prepare_config(self, c):
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

    def mail_admins(self, subject, body, fail_silently=False):
        """Send an e-mail to the admins in the :setting:`ADMINS` setting."""
        if self.conf.ADMINS:
            to = [admin_email for _, admin_email in self.conf.ADMINS]
            return self.loader.mail_admins(subject, body, fail_silently, to=to,
                                       sender=self.conf.SERVER_EMAIL,
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

    def merge(self, l, r):
        """Like `dict(a, **b)` except it will keep values from `a`
        if the value in `b` is :const:`None`."""
        return lpmerge(l, r)

    def _get_backend(self):
        from celery.backends import get_backend_cls
        backend_cls = self.backend_cls or self.conf.CELERY_RESULT_BACKEND
        backend_cls = get_backend_cls(backend_cls, loader=self.loader)
        return backend_cls(app=self)

    def _get_config(self):
        return ConfigurationView({},
                [self.prepare_config(self.loader.conf), deepcopy(DEFAULTS)])

    @cached_property
    def amqp(self):
        """Sending/receiving messages.  See :class:`~celery.app.amqp.AMQP`."""
        return instantiate(self.amqp_cls, app=self)

    @cached_property
    def backend(self):
        """Storing/retreiving task state.  See
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
        from celery.loaders import get_loader_cls
        return get_loader_cls(self.loader_cls)(app=self)

    @cached_property
    def log(self):
        """Logging utilities.  See :class:`~celery.log.Logging`."""
        return instantiate(self.log_cls, app=self)
