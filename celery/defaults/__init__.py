import os
import sys

from datetime import timedelta

from celery import routes
from celery.datastructures import AttributeDict
from celery.defaults.conf import DEFAULTS

def isatty(fh):
    # Fixes bug with mod_wsgi:
    #   mod_wsgi.Log object has no attribute isatty.
    return getattr(fh, "isatty", None) and fh.isatty()


class DefaultApp(object):
    _backend = None
    _conf = None
    _loader = None

    def __init__(self, loader=None, backend_cls=None):
        self.loader_cls = loader or os.environ.get("CELERY_LOADER", "default")
        self.backend_cls = backend_cls

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

    def broker_connection(self, **kwargs):
        from celery.messaging import establish_connection
        return establish_connection(app=self, **kwargs)

    def pre_config_merge(self, c):
        if not c.get("CELERY_RESULT_BACKEND"):
            c["CELERY_RESULT_BACKEND"] = c.get("CELERY_BACKEND")
        if not c.get("BROKER_BACKEND"):
            c["BROKER_BACKEND"] = c.get("BROKER_TRANSPORT")  or \
                                    c.get("CARROT_BACKEND")
        c.setdefault("CELERY_SEND_TASK_ERROR_EMAILS",
                     c.get("SEND_CELERY_TASK_ERROR_EMAILS"))
        return c

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

default_app = DefaultApp()


def app_or_default(app=None):
    if app is None:
        return default_app
    return app
