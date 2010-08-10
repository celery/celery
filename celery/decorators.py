"""

Decorators

"""
from inspect import getargspec

from celery import registry
from celery.task.base import Task, PeriodicTask
from celery.utils.functional import wraps


def task(*args, **options):
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
            base = options.pop("base", Task)

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


def periodic_task(**options):
    """Task decorator to create a periodic task.

    Example task, scheduling a task once every day:

    .. code-block:: python

        from datetime import timedelta

        @periodic_task(run_every=timedelta(days=1))
        def cronjob(**kwargs):
            logger = cronjob.get_logger(**kwargs)
            logger.warn("Task running...")

    """
    return task(**dict({"base": PeriodicTask}, **options))
