import os

from inspect import getargspec

from celery import registry
from celery.app import base
from celery.utils.functional import wraps


class App(base.BaseApp):

    def create_task_cls(self):
        from celery.task.base import create_task_cls
        return create_task_cls(app=self)

    def Worker(self, **kwargs):
        from celery.apps.worker import Worker
        return Worker(app=self, **kwargs)

    def Beat(self, **kwargs):
        from celery.apps.beat import Beat
        return Beat(app=self, **kwargs)

    def TaskSet(self, *args, **kwargs):
        from celery.task.sets import TaskSet
        kwargs["app"] = self
        return TaskSet(*args, **kwargs)

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
                return registry.tasks[T.name] # global instance.

            return _create_task_cls

        if len(args) == 1 and callable(args[0]):
            return inner_create_task_cls()(*args)
        return inner_create_task_cls(**options)

default_app = App()


if os.environ.get("CELERY_TRACE_APP"):
    def app_or_default(app=None):
        from multiprocessing import current_process
        if app is None:
            if current_process()._name == "MainProcess":
                raise Exception("DEFAULT APP")
            print("RETURNING TO DEFAULT APP")
            import traceback
            traceback.print_stack()
            return default_app
        return app
else:
    def app_or_default(app=None):
        if app is None:
            return default_app
        return app
