from inspect import getargspec

from billiard.utils.functional import wraps

from celery.task.base import Task, PeriodicTask


def task(**options):
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

    def _create_task_cls(fun):
        base = options.pop("base", Task)

        @wraps(fun)
        def run(self, *args, **kwargs):
            return fun(*args, **kwargs)
        run.argspec = getargspec(fun)

        cls_dict = dict(options, run=run, __module__=fun.__module__)
        return type(fun.__name__, (base, ), cls_dict)()

    return _create_task_cls


def periodic_task(**options):
    """Task decorator to create a periodic task.

    Run a task once every day:

    .. code-block:: python

        from datetime import timedelta

        @periodic_task(run_every=timedelta(days=1))
        def cronjob(**kwargs):
            logger = cronjob.get_logger(**kwargs)
            logger.warn("Task running...")

    """
    options.setdefault("base", PeriodicTask)
    return task(**options)
