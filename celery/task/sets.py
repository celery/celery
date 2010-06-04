from UserList import UserList

from celery import conf
from celery.messaging import establish_connection, with_connection
from celery.messaging import TaskPublisher
from celery.result import TaskSetResult
from celery.utils import gen_unique_id


class subtask(object):
    """A subtask part of a :class:`TaskSet`.

    :param task: The task class.
    :keyword args: Positional arguments to apply.
    :keyword kwargs: Keyword arguments to apply.
    :keyword options: Additional options to
      :func:`celery.execute.apply_async`.

    """

    def __init__(self, task, args=None, kwargs=None, options=None):
        self.task = task
        self.args = args or ()
        self.kwargs = kwargs or {}
        self.options = options or {}

    def apply(self, taskset_id):
        """Apply this task locally."""
        return self.task.apply(self.args, self.kwargs,
                               taskset_id=taskset_id, **self.options)

    def apply_async(self, taskset_id, publisher):
        """Apply this task asynchronously."""
        return self.task.apply_async(self.args, self.kwargs,
                                     taskset_id=taskset_id,
                                     publisher=publisher, **self.options)


class TaskSet(UserList):
    """A task containing several subtasks, making it possible
    to track how many, or when all of the tasks has been completed.

    :param tasks: A list of :class:`subtask`s.

    .. attribute:: total

        Total number of subtasks in this task set.

    Example

        >>> from djangofeeds.tasks import RefreshFeedTask
        >>> from celery.task.sets import TaskSet, subtask
        >>> urls = ("http://cnn.com/rss",
        ...         "http://bbc.co.uk/rss",
        ...         "http://xkcd.com/rss")
        >>> subtasks = [subtask(RefreshFeedTask, kwargs={"feed_url": url})
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

        :returns: A :class:`celery.result.TaskSetResult` instance.

        Example

            >>> ts = TaskSet(tasks=(
            ...         subtask(RefreshFeedTask, ["http://foo.com/rss"]),
            ...         subtask(RefreshFeedTask, ["http://bar.com/rss"]),
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
        conn = connection or establish_connection(connect_timeout=connect_timeout)
        publisher = TaskPublisher(connection=conn)
        try:
            results = [task.apply_async(taskset_id, publisher)
                            for task in self.tasks]
        finally:
            publisher.close()
            connection or conn.close()

        return TaskSetResult(taskset_id, results)

    def apply(self):
        """Applies the taskset locally."""
        taskset_id = gen_unique_id()

        # This will be filled with EagerResults.
        return TaskSetResult(taskset_id, [task.apply(taskset_id)
                                            for task in self.tasks])

    @property
    def tasks(self):
        return self.data
