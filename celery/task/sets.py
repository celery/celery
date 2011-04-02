import warnings

from kombu.utils import cached_property

from celery import registry
from celery.app import app_or_default
from celery.datastructures import AttributeDict
from celery.utils import gen_unique_id
from celery.utils.compat import UserList

TASKSET_DEPRECATION_TEXT = """\
Using this invocation of TaskSet is deprecated and will be removed
in Celery v2.4!

TaskSets now supports multiple types of tasks, the API has to reflect
this so the syntax has been changed to:

    from celery.task.sets import TaskSet

    ts = TaskSet(tasks=[
            %(cls)s.subtask(args1, kwargs1, options1),
            %(cls)s.subtask(args2, kwargs2, options2),
            ...
            %(cls)s.subtask(argsN, kwargsN, optionsN),
    ])

    result = ts.apply_async()

Thank you for your patience!

"""


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

    def __init__(self, task=None, args=None, kwargs=None, options=None, **ex):
        init = super(subtask, self).__init__

        if isinstance(task, dict):
            return init(task)  # works like dict(d)

        # Also supports using task class/instance instead of string name.
        try:
            task_name = task.name
        except AttributeError:
            task_name = task

        init(task=task_name, args=tuple(args or ()),
                             kwargs=dict(kwargs or {}, **ex),
                             options=options or {})

    def delay(self, *argmerge, **kwmerge):
        """Shortcut to `apply_async(argmerge, kwargs)`."""
        return self.apply_async(args=argmerge, kwargs=kwmerge)

    def apply(self, args=(), kwargs={}, **options):
        """Apply this task locally."""
        # For callbacks: extra args are prepended to the stored args.
        args = tuple(args) + tuple(self.args)
        kwargs = dict(self.kwargs, **kwargs)
        options = dict(self.options, **options)
        return self.type.apply(args, kwargs, **options)

    def apply_async(self, args=(), kwargs={}, **options):
        """Apply this task asynchronously."""
        # For callbacks: extra args are prepended to the stored args.
        args = tuple(args) + tuple(self.args)
        kwargs = dict(self.kwargs, **kwargs)
        options = dict(self.options, **options)
        return self.type.apply_async(args, kwargs, **options)

    def get_type(self):
        return self.type

    def __reduce__(self):
        # for serialization, the task type is lazily loaded,
        # and not stored in the dict itself.
        return (self.__class__, (dict(self), ), None)

    def __repr__(self, kwformat=lambda i: "%s=%r" % i, sep=', '):
        kw = self["kwargs"]
        return "%s(%s%s%s)" % (self["task"], sep.join(map(repr, self["args"])),
                kw and sep or "", sep.join(map(kwformat, kw.iteritems())))

    @cached_property
    def type(self):
        return registry.tasks[self.task]


class TaskSet(UserList):
    """A task containing several subtasks, making it possible
    to track how many, or when all of the tasks have been completed.

    :param tasks: A list of :class:`subtask` instances.

    Example::

        >>> urls = ("http://cnn.com/rss", "http://bbc.co.uk/rss")
        >>> taskset = TaskSet(refresh_feed.subtask((url, )) for url in urls)
        >>> taskset_result = taskset.apply_async()
        >>> list_of_return_values = taskset_result.join()  # *expensive*

    """
    _task = None                # compat
    _task_name = None           # compat

    #: Total number of subtasks in this set.
    total = None

    def __init__(self, task=None, tasks=None, app=None, Publisher=None):
        self.app = app_or_default(app)
        if task is not None:
            if hasattr(task, "__iter__"):
                tasks = task
            else:
                # Previously TaskSet only supported applying one kind of task.
                # the signature then was TaskSet(task, arglist),
                # so convert the arguments to subtasks'.
                tasks = [subtask(task, *arglist) for arglist in tasks]
                task = self._task = registry.tasks[task.name]
                self._task_name = task.name
                warnings.warn(TASKSET_DEPRECATION_TEXT % {
                                "cls": task.__class__.__name__},
                              DeprecationWarning)
        self.data = list(tasks or [])
        self.total = len(self.tasks)
        self.Publisher = Publisher or self.app.amqp.TaskPublisher

    def apply_async(self, connection=None, connect_timeout=None,
            publisher=None, taskset_id=None):
        """Apply taskset."""
        return self.app.with_default_connection(self._apply_async)(
                    connection=connection,
                    connect_timeout=connect_timeout,
                    publisher=publisher,
                    taskset_id=taskset_id)

    def _apply_async(self, connection=None, connect_timeout=None,
            publisher=None, taskset_id=None):
        if self.app.conf.CELERY_ALWAYS_EAGER:
            return self.apply(taskset_id=taskset_id)

        setid = taskset_id or gen_unique_id()
        pub = publisher or self.Publisher(connection=connection)
        try:
            results = self._async_results(setid, pub)
        finally:
            if not publisher:  # created by us.
                pub.close()

        return self.app.TaskSetResult(setid, results)

    def _async_results(self, taskset_id, publisher):
        return [task.apply_async(taskset_id=taskset_id, publisher=publisher)
                for task in self.tasks]

    def apply(self, taskset_id=None):
        """Applies the taskset locally by blocking until all tasks return."""
        setid = taskset_id or gen_unique_id()
        return self.app.TaskSetResult(setid, self._sync_results(setid))

    def _sync_results(self, taskset_id):
        return [task.apply(taskset_id=taskset_id) for task in self.tasks]

    @property
    def tasks(self):
        return self.data

    @property
    def task(self):
        warnings.warn(
            "TaskSet.task is deprecated and will be removed in 1.4",
            DeprecationWarning)
        return self._task

    @property
    def task_name(self):
        warnings.warn(
            "TaskSet.task_name is deprecated and will be removed in 1.4",
            DeprecationWarning)
        return self._task_name
