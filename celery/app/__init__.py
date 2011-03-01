"""
celery.app
==========

Celery Application.

:copyright: (c) 2009 - 2011 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
import os
import threading

from inspect import getargspec

from kombu.utils import cached_property

from celery import registry
from celery.app import base
from celery.utils import instantiate
from celery.utils.functional import wraps

# Apps with the :attr:`~celery.app.base.BaseApp.set_as_current` attribute
# sets this, so it will always contain the last instantiated app,
# and is the default app returned by :func:`app_or_default`.
_tls = threading.local()
_tls.current_app = None


class App(base.BaseApp):
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

    def set_current(self):
        _tls.current_app = self

    def on_init(self):
        if self.set_as_current:
            self.set_current()

    def create_task_cls(self):
        """Creates a base task class using default configuration
        taken from this app."""
        conf = self.conf

        from celery.task.base import BaseTask

        class Task(BaseTask):
            abstract = True
            app = self
            backend = self.backend
            exchange_type = conf.CELERY_DEFAULT_EXCHANGE_TYPE
            delivery_mode = conf.CELERY_DEFAULT_DELIVERY_MODE
            send_error_emails = conf.CELERY_SEND_TASK_ERROR_EMAILS
            error_whitelist = conf.CELERY_TASK_ERROR_WHITELIST
            serializer = conf.CELERY_TASK_SERIALIZER
            rate_limit = conf.CELERY_DEFAULT_RATE_LIMIT
            track_started = conf.CELERY_TRACK_STARTED
            acks_late = conf.CELERY_ACKS_LATE
            ignore_result = conf.CELERY_IGNORE_RESULT
            store_errors_even_if_ignored = \
                conf.CELERY_STORE_ERRORS_EVEN_IF_IGNORED
            accept_magic_kwargs = self.accept_magic_kwargs
        Task.__doc__ = BaseTask.__doc__

        return Task

    def Worker(self, **kwargs):
        """Create new :class:`~celery.apps.worker.Worker` instance."""
        return instantiate("celery.apps.worker.Worker", app=self, **kwargs)

    def Beat(self, **kwargs):
        """Create new :class:`~celery.apps.beat.Beat` instance."""
        return instantiate("celery.apps.beat.Beat", app=self, **kwargs)

    def TaskSet(self, *args, **kwargs):
        """Create new :class:`~celery.task.sets.TaskSet`."""
        from celery.task.sets import TaskSet
        kwargs["app"] = self
        return TaskSet(*args, **kwargs)

    def worker_main(self, argv=None):
        """Run :program:`celeryd` using `argv`.  Uses :data:`sys.argv`
        if `argv` is not specified."""
        from celery.bin.celeryd import WorkerCommand
        return WorkerCommand(app=self).execute_from_commandline(argv)

    def task(self, *args, **options):
        """Decorator to create a task class out of any callable.

        .. admonition:: Examples

            .. code-block:: python

                @task()
                def refresh_feed(url):
                    return Feed.objects.get(url=url).refresh()

            With setting extra options and using retry.

            .. code-block:: python

                @task(exchange="feeds")
                def refresh_feed(url, **kwargs):
                    try:
                        return Feed.objects.get(url=url).refresh()
                    except socket.error, exc:
                        refresh_feed.retry(args=[url], kwargs=kwargs, exc=exc)

            Calling the resulting task:

                >>> refresh_feed("http://example.com/rss") # Regular
                <Feed: http://example.com/rss>
                >>> refresh_feed.delay("http://example.com/rss") # Async
                <AsyncResult: 8998d0f4-da0b-4669-ba03-d5ab5ac6ad5d>

        """

        def inner_create_task_cls(**options):

            def _create_task_cls(fun):
                options["app"] = self
                options.setdefault("accept_magic_kwargs", False)
                base = options.pop("base", None) or self.Task

                @wraps(fun, assigned=("__module__", "__name__"))
                def run(self, *args, **kwargs):
                    return fun(*args, **kwargs)

                # Save the argspec for this task so we can recognize
                # which default task kwargs we're going to pass to it later.
                # (this happens in celery.utils.fun_takes_kwargs)
                run.argspec = getargspec(fun)

                cls_dict = dict(options, run=run,
                                __module__=fun.__module__,
                                __doc__=fun.__doc__)
                T = type(fun.__name__, (base, ), cls_dict)()
                return registry.tasks[T.name]             # global instance.

            return _create_task_cls

        if len(args) == 1 and callable(args[0]):
            return inner_create_task_cls(**options)(*args)
        return inner_create_task_cls(**options)

    @cached_property
    def Task(self):
        """Default Task base class for this application."""
        return self.create_task_cls()

    def __repr__(self):
        return "<Celery: %s:0x%x>" % (self.main or "__main__", id(self), )

    def __reduce__(self):
        # Reduce only pickles the configuration changes,
        # so the default configuration doesn't have to be passed
        # between processes.
        return (_unpickle_app, (self.__class__,
                                self.main,
                                self.conf.changes,
                                self.loader_cls,
                                self.backend_cls,
                                self.amqp_cls,
                                self.events_cls,
                                self.log_cls,
                                self.control_cls,
                                self.accept_magic_kwargs))


def _unpickle_app(cls, main, changes, loader, backend, amqp,
        events, log, control, accept_magic_kwargs):
    app = cls(main, loader=loader, backend=backend, amqp=amqp,
                    events=events, log=log, control=control,
                    set_as_current=False,
                    accept_magic_kwargs=accept_magic_kwargs)
    app.conf.update(changes)
    return app


#: The "default" loader is the default loader used by old applications.
default_loader = os.environ.get("CELERY_LOADER") or "default"

#: Global fallback app instance.
default_app = App("default", loader=default_loader,
                  set_as_current=False, accept_magic_kwargs=True)


def current_app():
    return getattr(_tls, "current_app", None) or default_app


def _app_or_default(app=None):
    """Returns the app provided or the default app if none.

    The environment variable :envvar:`CELERY_TRACE_APP` is used to
    trace app leaks.  When enabled an exception is raised if there
    is no active app.

    """
    if app is None:
        return getattr(_tls, "current_app", None) or default_app
    return app


def _app_or_default_trace(app=None):  # pragma: no cover
    from traceback import print_stack
    from multiprocessing import current_process
    if app is None:
        if getattr(_tls, "current_app", None):
            print("-- RETURNING TO CURRENT APP --")
            print_stack()
            return _tls.current_app
        if current_process()._name == "MainProcess":
            raise Exception("DEFAULT APP")
        print("-- RETURNING TO DEFAULT APP --")
        print_stack()
        return default_app
    return app


def enable_trace():
    global app_or_default
    app_or_default = _app_or_default_trace


def disable_trace():
    global app_or_default
    app_or_default = _app_or_default


app_or_default = _app_or_default
if os.environ.get("CELERY_TRACE_APP"):  # pragma: no cover
    enable_trace()
