from UserList import UserList

from celery import conf
from celery import registry
from celery.datastructures import AttributeDict
from celery.messaging import establish_connection, with_connection
from celery.messaging import TaskPublisher
from celery.result import TaskSetResult
from celery.utils import gen_unique_id


class subtask(AttributeDict):
    """Class that wraps the arguments and execution options
    for a single task invocation.

    Used as the parts in a :class:`TaskSet` or to safely
    pass tasks around as callbacks.

    :param task: Either a task class/instance, or the name of a task.
    :keyword args: Positional arguments to apply.
    :keyword kwargs: Keyword arguments to apply.
    :keyword options: Additional options to
      :func:`celery.execute.apply_async`.

    Note that if the first argument is a :class:`dict`, the other
    arguments will be ignored and the values in the dict will be used
    instead.

        >>> s = subtask("tasks.add", args=(2, 2))
        >>> subtask(s)
        {"task": "tasks.add", args=(2, 2), kwargs={}, options={}}

    """

    def __init__(self, task=None, args=None, kwargs=None, options=None,
            **extra):
        init = super(subtask, self).__init__

        if isinstance(task, dict):
            # Use the values from a dict.
            return init(task)

        # Also supports using task class/instance instead of string name.
        try:
            task_name = task.name
        except AttributeError:
            task_name = task

        init(task=task_name, args=tuple(args or ()), kwargs=kwargs or (),
             options=options or ())

    def apply(self, *argmerge, **execopts):
        """Apply this task locally."""
        # For callbacks: extra args are prepended to the stored args.
        args = tuple(argmerge) + tuple(self.args)
        return self.get_type().apply(args, self.kwargs,
                                     **dict(self.options, **execopts))

    def apply_async(self, *argmerge, **execopts):
        """Apply this task asynchronously."""
        # For callbacks: extra args are prepended to the stored args.
        args = tuple(argmerge) + tuple(self.args)
        return self.get_type().apply_async(args, self.kwargs,
                                           **dict(self.options, **execopts))

    def get_type(self):
        # For JSON serialization, the task class is lazily loaded,
        # and not stored in the dict itself.
        return registry.tasks[self.task]


class TaskSet(UserList):
    """A task containing several subtasks, making it possible
    to track how many, or when all of the tasks has been completed.

    :param tasks: A list of :class:`subtask` instances.

    .. attribute:: total

        Total number of subtasks in this task set.

    Example::

        >>> from djangofeeds.tasks import RefreshFeedTask
        >>> from celery.task.sets import TaskSet, subtask
        >>> urls = ("http://cnn.com/rss",
        ...         "http://bbc.co.uk/rss",
        ...         "http://xkcd.com/rss")
        >>> subtasks = [RefreshFeedTask.subtask(kwargs={"feed_url": url})
        ...                 for url in urls]
        >>> taskset = TaskSet(tasks=subtasks)
        >>> taskset_result = taskset.apply_async()
        >>> list_of_return_values = taskset_result.join()

    """
    task = None # compat
    task_name = None # compat

    def __init__(self, task=None, tasks=None):
        # Previously TaskSet only supported applying one kind of task.
        # the signature then was TaskSet(task, arglist)
        # Convert the arguments to subtasks'.
        if task is not None:
            tasks = [subtask(task, *arglist) for arglist in tasks]
            self.task = task
            self.task_name = task.name

        self.data = list(tasks)
        self.total = len(self.tasks)

    @with_connection
    def apply_async(self, connection=None,
            connect_timeout=conf.BROKER_CONNECTION_TIMEOUT):
        """Run all tasks in the taskset.

        Returns a :class:`celery.result.TaskSetResult` instance.

        Example

            >>> ts = TaskSet(tasks=(
            ...         RefreshFeedTask.subtask(["http://foo.com/rss"]),
            ...         RefreshFeedTask.subtask(["http://bar.com/rss"]),
            ... ))
            >>> result = ts.apply_async()
            >>> result.taskset_id
            "d2c9b261-8eff-4bfb-8459-1e1b72063514"
            >>> result.subtask_ids
            ["b4996460-d959-49c8-aeb9-39c530dcde25",
            "598d2d18-ab86-45ca-8b4f-0779f5d6a3cb"]
            >>> result.waiting()
            True
            >>> time.sleep(10)
            >>> result.ready()
            True
            >>> result.successful()
            True
            >>> result.failed()
            False
            >>> result.join()
            [True, True]

        """
        if conf.ALWAYS_EAGER:
            return self.apply()

        taskset_id = gen_unique_id()
        publisher = TaskPublisher(connection=connection)
        try:
            results = [task.apply_async(taskset_id=taskset_id,
                                        publisher=publisher)
                            for task in self.tasks]
        finally:
            publisher.close()

        return TaskSetResult(taskset_id, results)

    def apply(self):
        """Applies the taskset locally."""
        taskset_id = gen_unique_id()

        # This will be filled with EagerResults.
        return TaskSetResult(taskset_id, [task.apply(taskset_id=taskset_id)
                                            for task in self.tasks])

    @property
    def tasks(self):
        return self.data
