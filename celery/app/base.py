import os
import sys

from datetime import timedelta
from itertools import chain

from celery import routes
from celery.app.defaults import DEFAULTS
from celery.datastructures import AttributeDictMixin
from celery.utils import noop, isatty
from celery.utils.functional import wraps


class MultiDictView(AttributeDictMixin):
    """View for one more more dicts.

    * When getting a key, the dicts are searched in order.
    * When setting a key, the key is added to the first dict.

    >>> d1 = {"x": 3"}
    >>> d2 = {"x": 1, "y": 2, "z": 3}
    >>> x = MultiDictView([d1, d2])

    >>> x["x"]
    3

    >>>  x["y"]
    2

    """
    dicts = None

    def __init__(self, *dicts):
        self.__dict__["dicts"] = dicts

    def __getitem__(self, key):
        for d in self.__dict__["dicts"]:
            try:
                return d[key]
            except KeyError:
                pass
        raise KeyError(key)

    def __setitem__(self, key, value):
        self.__dict__["dicts"][0][key] = value

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def setdefault(self, key, default):
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    def update(self, *args, **kwargs):
        return self.__dict__["dicts"][0].update(*args, **kwargs)

    def __contains__(self, key):
        for d in self.__dict__["dicts"]:
            if key in d:
                return True
        return False

    def __repr__(self):
        return repr(dict(iter(self)))

    def __iter__(self):
        return chain(*[d.iteritems() for d in self.__dict__["dicts"]])


class BaseApp(object):
    """Base class for apps."""

    def __init__(self, loader=None, backend=None, set_as_current=True):
        self.loader_cls = loader or "app"
        self.backend_cls = backend
        self._amqp = None
        self._backend = None
        self._conf = None
        self._control = None
        self._loader = None
        self._log = None
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
        self._conf = None
        return self.loader.config_from_object(obj, silent=silent)

    def config_from_envvar(self, variable_name, silent=False):
        """Read configuration from environment variable.

        The value of the environment variable must be the name
        of an object to import.

            >>> os.environ["CELERY_CONFIG_MODULE"] = "myapp.celeryconfig"
            >>> celery.config_from_envvar("CELERY_CONFIG_MODULE")

        """
        self._conf = None
        return self.loader.config_from_envvar(variable_name, silent=silent)

    def config_from_cmdline(self, argv, namespace="celery"):
        """Read configuration from argv.

        The config

        """
        config = self.loader.cmdline_config_parser(argv, namespace)
        for key, value in config.items():
            self.conf[key] = value

    def either(self, default_key, *values):
        """Fallback to the value of a configuration key if none of the
        ``*values`` are true."""
        for value in values:
            if value is not None:
                return value
        return self.conf.get(default_key)

    def merge(self, a, b):
        """Like ``dict(a, **b)`` except it will keep values from ``a``
        if the value in ``b`` is :const:`None`."""
        b = dict(b)
        for key, value in a.items():
            if b.get(key) is None:
                b[key] = value
        return b

    def send_task(self, name, args=None, kwargs=None, countdown=None,
            eta=None, task_id=None, publisher=None, connection=None,
            connect_timeout=None, result_cls=None, expires=None,
            **options):
        """Send task by name.

        :param name: Name of task to execute (e.g. ``"tasks.add"``).
        :keyword result_cls: Specify custom result class. Default is
            using :meth:`AsyncResult`.

        Supports the same arguments as
        :meth:`~celery.task.base.BaseTask.apply_async`.

        """
        result_cls = result_cls or self.AsyncResult
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

    def broker_connection(self, hostname=None, userid=None,
            password=None, virtual_host=None, port=None, ssl=None,
            insist=None, connect_timeout=None, backend_cls=None):
        """Establish a connection to the message broker.

        :keyword hostname: defaults to the ``BROKER_HOST`` setting.
        :keyword userid: defaults to the ``BROKER_USER`` setting.
        :keyword password: defaults to the ``BROKER_PASSWORD`` setting.
        :keyword virtual_host: defaults to the ``BROKER_VHOST`` setting.
        :keyword port: defaults to the ``BROKER_PORT`` setting.
        :keyword ssl: defaults to the ``BROKER_USE_SSL`` setting.
        :keyword insist: defaults to the ``BROKER_INSIST`` setting.
        :keyword connect_timeout: defaults to the
            ``BROKER_CONNECTION_TIMEOUT`` setting.
        :keyword backend_cls: defaults to the ``BROKER_BACKEND`` setting.

        :returns :class:`carrot.connection.BrokerConnection`:

        """
        return self.amqp.BrokerConnection(
                    hostname or self.conf.BROKER_HOST,
                    userid or self.conf.BROKER_USER,
                    password or self.conf.BROKER_PASSWORD,
                    virtual_host or self.conf.BROKER_VHOST,
                    port or self.conf.BROKER_PORT,
                    backend_cls=backend_cls or self.conf.BROKER_BACKEND,
                    insist=self.either("BROKER_INSIST", insist),
                    ssl=self.either("BROKER_USE_SSL", ssl),
                    connect_timeout=self.either(
                                "BROKER_CONNECTION_TIMEOUT", connect_timeout))

    def with_default_connection(self, fun):
        """With any function accepting ``connection`` and ``connect_timeout``
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
                c["CELERY_RESULT_BACKEND"] = backend
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
        if isinstance(c.CELERY_TASK_RESULT_EXPIRES, int):
            c["CELERY_TASK_RESULT_EXPIRES"] = timedelta(
                    seconds=c.CELERY_TASK_RESULT_EXPIRES)
        return c

    def mail_admins(self, subject, message, fail_silently=False):
        """Send an e-mail to the admins in conf.ADMINS."""
        from celery.utils import mail

        if not self.conf.ADMINS:
            return

        to = [admin_email for _, admin_email in self.conf.ADMINS]
        message = mail.Message(sender=self.conf.SERVER_EMAIL,
                               to=to, subject=subject, body=message)

        mailer = mail.Mailer(self.conf.EMAIL_HOST,
                             self.conf.EMAIL_PORT,
                             self.conf.EMAIL_HOST_USER,
                             self.conf.EMAIL_HOST_PASSWORD)
        mailer.send(message, fail_silently=fail_silently)

    def AsyncResult(self, task_id, backend=None):
        """Create :class:`celery.result.BaseAsyncResult` instance."""
        from celery.result import BaseAsyncResult
        return BaseAsyncResult(task_id, app=self,
                               backend=backend or self.backend)

    def TaskSetResult(self, taskset_id, results, **kwargs):
        """Create :class:`celery.result.TaskSetResult` instance."""
        from celery.result import TaskSetResult
        return TaskSetResult(taskset_id, results, app=self)

    def _get_backend(self):
        from celery.backends import get_backend_cls
        backend_cls = self.backend_cls or self.conf.CELERY_RESULT_BACKEND
        backend_cls = get_backend_cls(backend_cls, loader=self.loader)
        return backend_cls(app=self)

    def _get_config(self):
        return self.post_config_merge(MultiDictView(
                    self.pre_config_merge(self.loader.conf), DEFAULTS))

    @property
    def amqp(self):
        if self._amqp is None:
            from celery.app.amqp import AMQP
            self._amqp = AMQP(self)
        return self._amqp

    @property
    def backend(self):
        if self._backend is None:
            self._backend = self._get_backend()
        return self._backend

    @property
    def loader(self):
        if self._loader is None:
            from celery.loaders import get_loader_cls
            self._loader = get_loader_cls(self.loader_cls)(app=self)
        return self._loader

    @property
    def conf(self):
        if self._conf is None:
            self._conf = self._get_config()
        return self._conf

    @property
    def control(self):
        if self._control is None:
            from celery.task.control import Control
            self._control = Control(app=self)
        return self._control

    @property
    def log(self):
        if self._log is None:
            from celery.log import Logging
            self._log = Logging(app=self)
        return self._log
