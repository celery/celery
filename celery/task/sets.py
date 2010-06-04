from celery import conf
from celery.execute import apply_async
from celery.registry import tasks
from celery.result import TaskSetResult
from celery.utils import gen_unique_id, padlist


class TaskSet(object):
    """A task containing several subtasks, making it possible
    to track how many, or when all of the tasks has been completed.

    :param task: The task class or name.
        Can either be a fully qualified task name, or a task class.

    :param args: A list of args, kwargs pairs.
        e.g. ``[[args1, kwargs1], [args2, kwargs2], ..., [argsN, kwargsN]]``


    .. attribute:: task_name

        The name of the task.

    .. attribute:: arguments

        The arguments, as passed to the task set constructor.

    .. attribute:: total

        Total number of tasks in this task set.

    Example

        >>> from djangofeeds.tasks import RefreshFeedTask
        >>> taskset = TaskSet(RefreshFeedTask, args=[
        ...                 ([], {"feed_url": "http://cnn.com/rss"}),
        ...                 ([], {"feed_url": "http://bbc.com/rss"}),
        ...                 ([], {"feed_url": "http://xkcd.com/rss"})
        ... ])

        >>> taskset_result = taskset.apply_async()
        >>> list_of_return_values = taskset_result.join()

    """

    def __init__(self, task, args):
        try:
            task_name = task.name
            task_obj = task
        except AttributeError:
            task_name = task
            task_obj = tasks[task_name]

        # Get task instance
        task_obj = tasks[task_obj.name]

        self.task = task_obj
        self.task_name = task_name
        self.arguments = args
        self.total = len(args)

    def apply_async(self, connect_timeout=conf.BROKER_CONNECTION_TIMEOUT):
        """Run all tasks in the taskset.

        :returns: A :class:`celery.result.TaskSetResult` instance.

        Example

            >>> ts = TaskSet(RefreshFeedTask, args=[
            ...         (["http://foo.com/rss"], {}),
            ...         (["http://bar.com/rss"], {}),
            ... ])
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
        conn = self.task.establish_connection(connect_timeout=connect_timeout)
        publisher = self.task.get_publisher(connection=conn)
        try:
            subtasks = [self.apply_part(arglist, taskset_id, publisher)
                            for arglist in self.arguments]
        finally:
            publisher.close()
            conn.close()

        return TaskSetResult(taskset_id, subtasks)

    def apply_part(self, arglist, taskset_id, publisher):
        """Apply a single part of the taskset."""
        args, kwargs, opts = padlist(arglist, 3, default={})
        return apply_async(self.task, args, kwargs,
                           taskset_id=taskset_id, publisher=publisher, **opts)

    def apply(self):
        """Applies the taskset locally."""
        taskset_id = gen_unique_id()
        subtasks = [apply(self.task, args, kwargs)
                        for args, kwargs in self.arguments]

        # This will be filled with EagerResults.
        return TaskSetResult(taskset_id, subtasks)
