import os
import sys

from datetime import timedelta

from carrot.connection import BrokerConnection

from celery import routes
from celery.app.defaults import DEFAULTS
from celery.datastructures import AttributeDict
from celery.utils import noop, isatty
from celery.utils.functional import wraps


class BaseApp(object):
    _backend = None
    _conf = None
    _control = None
    _loader = None

    def __init__(self, loader=None, backend_cls=None):
        self.loader_cls = loader or os.environ.get("CELERY_LOADER", "default")
        self.backend_cls = backend_cls

    def either(self, default_key, *values):
        for value in values:
            if value is not None:
                return value
        return self.conf.get(default_key)

    def get_queues(self):
        c = self.conf
        queues = c.CELERY_QUEUES

        def _defaults(opts):
            opts.setdefault("exchange", c.CELERY_DEFAULT_EXCHANGE),
            opts.setdefault("exchange_type", c.CELERY_DEFAULT_EXCHANGE_TYPE)
            opts.setdefault("binding_key", c.CELERY_DEFAULT_EXCHANGE)
            opts.setdefault("routing_key", opts.get("binding_key"))
            return opts

        return dict((queue, _defaults(opts))
                    for queue, opts in queues.items())

    def get_default_queue(self):
        q = self.conf.CELERY_DEFAULT_QUEUE
        return q, self.get_queues()[q]

    def broker_connection(self, hostname=None, userid=None,
            password=None, virtual_host=None, port=None, ssl=None,
            insist=None, connect_timeout=None, backend_cls=None):
        """Establish a connection to the message broker."""
        return BrokerConnection(
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

    def get_consumer_set(self, connection, queues=None, **options):
        from celery.messaging import ConsumerSet, Consumer

        queues = queues or self.get_queues()

        cset = ConsumerSet(connection)
        for queue_name, queue_options in queues.items():
            queue_options = dict(queue_options)
            queue_options["routing_key"] = queue_options.pop("binding_key",
                                                             None)
            consumer = Consumer(connection, queue=queue_name,
                                backend=cset.backend, **queue_options)
            cset.consumers.append(consumer)
        return cset

    def pre_config_merge(self, c):
        if not c.get("CELERY_RESULT_BACKEND"):
            c["CELERY_RESULT_BACKEND"] = c.get("CELERY_BACKEND")
        if not c.get("BROKER_BACKEND"):
            c["BROKER_BACKEND"] = c.get("BROKER_TRANSPORT")  or \
                                    c.get("CARROT_BACKEND")
        c.setdefault("CELERY_SEND_TASK_ERROR_EMAILS",
                     c.get("SEND_CELERY_TASK_ERROR_EMAILS"))
        return c

    def post_config_merge(self, c):
        if not c.get("CELERY_QUEUES"):
            c["CELERY_QUEUES"] = {c.CELERY_DEFAULT_QUEUE: {
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

    @property
    def backend(self):
        if self._backend is None:
            from celery.backends import get_backend_cls
            backend_cls = self.backend_cls or self.conf.CELERY_RESULT_BACKEND
            backend_cls = get_backend_cls(backend_cls, loader=self.loader)
            self._backend = backend_cls(app=self)
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
            config = self.pre_config_merge(self.loader.conf)
            self._conf = self.post_config_merge(
                            AttributeDict(DEFAULTS, **config))
        return self._conf

    @property
    def control(self):
        if self._control is None:
            from celery.task.control import Control
            self._control = Control(app=self)
        return self._control
