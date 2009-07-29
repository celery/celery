"""

Django Models.

"""
from django.db import models
from celery.registry import tasks
from celery.managers import TaskManager, PeriodicTaskManager
from celery.fields import PickledObjectField
from django.utils.translation import ugettext_lazy as _
from datetime import datetime

TASK_STATUS_PENDING = "PENDING"
TASK_STATUS_RETRY = "RETRY"
TASK_STATUS_FAILURE = "FAILURE"
TASK_STATUS_DONE = "DONE"
TASK_STATUSES = (TASK_STATUS_PENDING, TASK_STATUS_RETRY,
                 TASK_STATUS_FAILURE, TASK_STATUS_DONE)
TASK_STATUSES_CHOICES = zip(TASK_STATUSES, TASK_STATUSES)


class TaskMeta(models.Model):
    """Task result/status."""
    task_id = models.CharField(_(u"task id"), max_length=255, unique=True)
    status = models.CharField(_(u"task status"), max_length=50,
            default=TASK_STATUS_PENDING, choices=TASK_STATUSES_CHOICES)
    result = PickledObjectField()
    date_done = models.DateTimeField(_(u"done at"), auto_now=True)

    objects = TaskManager()

    class Meta:
        """Model meta-data."""
        verbose_name = _(u"task meta")
        verbose_name_plural = _(u"task meta")

    def __unicode__(self):
        return u"<Task: %s done:%s>" % (self.task_id, self.status)


class PeriodicTaskMeta(models.Model):
    """Information about a Periodic Task."""
    name = models.CharField(_(u"name"), max_length=255, unique=True)
    last_run_at = models.DateTimeField(_(u"last time run"),
                                       blank=True, 
                                       default=datetime.fromtimestamp(0))
    total_run_count = models.PositiveIntegerField(_(u"total run count"),
                                                  default=0)

    objects = PeriodicTaskManager()

    class Meta:
        """Model meta-data."""
        verbose_name = _(u"periodic task")
        verbose_name_plural = _(u"periodic tasks")

    def __unicode__(self):
        return u"<PeriodicTask: %s [last-run:%s, total-run:%d]>" % (
                self.name, self.last_run_at, self.total_run_count)

    def delay(self, *args, **kwargs):
        """Apply the periodic task immediately."""
        self.task.delay()
        self.total_run_count = self.total_run_count + 1
        self.save()

    @property
    def task(self):
        """The entry registered in the task registry for this task."""
        return tasks[self.name]
