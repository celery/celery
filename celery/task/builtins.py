from celery import conf
from celery.schedules import crontab
from celery.task.base import Task


class backend_cleanup(Task):
    name = "celery.backend_cleanup"

    def run(self):
        self.backend.cleanup()

if conf.TASK_RESULT_EXPIRES and \
        backend_cleanup.name not in conf.CELERYBEAT_SCHEDULE:
    conf.CELERYBEAT_SCHEDULE[backend_cleanup.name] = dict(
            task=backend_cleanup.name,
            schedule=crontab(minute="00", hour="04", day_of_week="*"))


DeleteExpiredTaskMetaTask = backend_cleanup         # FIXME remove in 3.0


class PingTask(Task):
    """The task used by :func:`ping`."""
    name = "celery.ping"

    def run(self, **kwargs):
        """:returns: the string `"pong"`."""
        return "pong"
