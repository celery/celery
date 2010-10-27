import os

from inspect import getargspec

from celery import registry
from celery.app import base
from celery.utils.functional import wraps

_current_app = None


class App(base.BaseApp):
    """Celery Application.

    Inherits from :class:`celery.app.base.BaseApp`.

    :keyword loader: The loader class, or the name of the loader class to use.
        Default is :class:`celery.loaders.app.AppLoader`.
    :keyword backend: The result store backend class, or the name of the
        backend class to use. Default is the value of the
        :setting:`CELERY_RESULT_BACKEND` setting.

    .. attribute:: amqp

        Sending/receiving messages.
        See :class:`celery.app.amqp.AMQP`.

    .. attribute:: backend

        Storing/retreiving task state.
        See :class:`celery.backend.base.BaseBackend`.

    .. attribute:: conf

        Current configuration. Supports both the dict interface and
        attribute access.

    .. attribute:: control

        Controlling worker nodes.
        See :class:`celery.task.control.Control`.

    .. attribute:: log

        Logging.
        See :class:`celery.log.Logging`.

    """

    def on_init(self):
        if self.set_as_current:
            global _current_app
            _current_app = self

    def create_task_cls(self):
        """Creates a base task class using default configuration
        taken from this app."""
        from celery.task.base import create_task_cls
        return create_task_cls(app=self)

    def Worker(self, **kwargs):
        """Create new :class:`celery.apps.worker.Worker` instance."""
        from celery.apps.worker import Worker
        return Worker(app=self, **kwargs)

    def Beat(self, **kwargs):
        """Create new :class:`celery.apps.beat.Beat` instance."""
        from celery.apps.beat import Beat
        return Beat(app=self, **kwargs)

    def TaskSet(self, *args, **kwargs):
        """Create new :class:`celery.task.sets.TaskSet`."""
        from celery.task.sets import TaskSet
        kwargs["app"] = self
        return TaskSet(*args, **kwargs)

    def worker_main(self, argv=None):
        from celery.bin.celeryd import WorkerCommand
        return WorkerCommand(app=self).execute_from_commandline(argv)

    def task(self, *args, **options):
        """Decorator to create a task class out of any callable.

        Examples:

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
                base = options.pop("base", None) or self.create_task_cls()

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
            return inner_create_task_cls()(*args)
        return inner_create_task_cls(**options)

# The "default" loader is the default loader used by old applications.
default_loader = os.environ.get("CELERY_LOADER") or "default"
default_app = App(loader=default_loader, set_as_current=False)

if os.environ.get("CELERY_TRACE_APP"):

    def app_or_default(app=None):
        from traceback import print_stack
        from multiprocessing import current_process
        global _current_app
        if app is None:
            if _current_app:
                print("-- RETURNING TO CURRENT APP --")
                print_stack()
                return _current_app
            if current_process()._name == "MainProcess":
                raise Exception("DEFAULT APP")
            print("-- RETURNING TO DEFAULT APP --")
            print_stack()
            return default_app
        return app
else:
    def app_or_default(app=None):
        """Returns the app provided or the default app if none.

        If the environment variable :envvar:`CELERY_TRACE_APP` is set,
        any time there is no active app and exception is raised. This
        is used to trace app leaks (when someone forgets to pass
        along the app instance).

        """
        global _current_app
        if app is None:
            return _current_app or default_app
        return app
