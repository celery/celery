from celery.task.base import Task, PeriodicTask
from celery.registry import tasks
from celery.backends import default_backend
from datetime import timedelta


class DeleteExpiredTaskMetaTask(PeriodicTask):
    """A periodic task that deletes expired task metadata every day.

    This runs the current backend's
    :meth:`celery.backends.base.BaseBackend.cleanup` method.

    """
    name = "celery.delete_expired_task_meta"
    run_every = timedelta(days=1)

    def run(self, **kwargs):
        """The method run by ``celeryd``."""
        logger = self.get_logger(**kwargs)
        logger.info("Deleting expired task meta objects...")
        default_backend.cleanup()


class PingTask(Task):
    """The task used by :func:`ping`."""
    name = "celery.ping"

    def run(self, **kwargs):
        """:returns: the string ``"pong"``."""
        return "pong"
