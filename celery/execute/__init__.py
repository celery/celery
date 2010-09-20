from celery.app import app_or_default
from celery.registry import tasks


def apply_async(task, *args, **kwargs):
    """Deprecated. See :meth:`celery.task.base.BaseTask.apply_async`."""
    # FIXME Deprecate!
    return task.apply_async(*args, **kwargs)


def apply(task, *args, **kwargs):
    """Deprecated. See :meth:`celery.task.base.BaseTask.apply`."""
    # FIXME Deprecate!
    return task.apply(*args, **kwargs)


def send_task(*args, **kwargs):
    """Deprecated. See :meth:`celery.app.App.send_task`."""
    # FIXME Deprecate!
    return app_or_default().send_task(*args, **kwargs)


def delay_task(task_name, *args, **kwargs):
    # FIXME Deprecate!
    return tasks[task_name].apply_async(args, kwargs)
