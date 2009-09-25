from celery.task.base import Task
from celery.registry import tasks


def task(**options):
    """Make a task out of any callable.

        :keyword name: Use a custom name, if not explicitly set
            the module and function name will be used, and
            the function name will be made into a standard class name,
            e.g. ``some_long_name`` becomes ``SomeLongName``.

        :keyword autoregister: Automatically register the task in the
            task registry.


        Examples:

            >>> @task()
            ... def refresh_feed(url):
            ...     return Feed.objects.get(url=url).refresh()


            >>> refresh_feed("http://example.com/rss") # Regular
            <Feed: http://example.com/rss>
            >>> refresh_feed.delay("http://example.com/rss") # Async
            <AsyncResult: 8998d0f4-da0b-4669-ba03-d5ab5ac6ad5d>

            # With setting extra options and using retry.

            >>> @task(exchange="feeds")
            ... def refresh_feed(url, **kwargs):
            ...     try:
            ...         return Feed.objects.get(url=url).refresh()
            ...     except socket.error, exc:
            ...         refresh_feed.retry(args=[url], kwargs=kwargs,
            ...                            exc=exc)


    """

    def _create_task_cls(fun):
        name = options.pop("name", None)
        autoregister = options.pop("autoregister", True)

        cls_name = "".join(map(str.capitalize, fun.__name__.split("_")))

        def run(self, *args, **kwargs):
            return fun(*args, **kwargs)
        run.__name__ = fun.__name__

        cls_dict = dict(options)
        cls_dict["run"] = run
        cls_dict["__module__"] = fun.__module__

        task = type(cls_name, (Task, ), cls_dict)()
        autoregister and tasks.register(task)

        return task()

    return _create_task_cls
