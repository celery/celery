from django.db import models
from celery.registry import tasks
from celery.managers import TaskManager, PeriodicTaskManager
from django.utils.translation import ugettext_lazy as _


class TaskMeta(models.Model):
    task_id = models.CharField(_(u"task id"), max_length=255, unique=True)
    is_done = models.BooleanField(_(u"is done"), default=False)
    date_done = models.DateTimeField(_(u"done at"), auto_now=True)

    objects = TaskManager()

    class Meta:
        verbose_name = _(u"task meta")
        verbose_name_plural = _(u"task meta")

    def __unicode__(self):
        return u"<Task: %s done:%s>" % (self.task_id, self.is_done)


class PeriodicTaskMeta(models.Model):
    name = models.CharField(_(u"name"), max_length=255, unique=True)
    last_run_at = models.DateTimeField(_(u"last time run"),
                                       auto_now=True, blank=True)
    total_run_count = models.PositiveIntegerField(_(u"total run count"),
                                                  default=0)

    objects = PeriodicTaskManager()

    class Meta:
        verbose_name = _(u"periodic task")
        verbose_name_plural = _(u"periodic tasks")

    def __unicode__(self):
        return u"<PeriodicTask: %s [last-run:%s, total-run:%d]>" % (
                self.name, self.last_run_at, self.total_run_count)

    def delay(self, **kwargs):
        self.task.delay()
        self.total_run_count = self.total_run_count + 1
        self.save()

    @property
    def task(self):
        return tasks[self.name]
