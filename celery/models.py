import django
from django.db import models
from django.utils.translation import ugettext_lazy as _

from picklefield.fields import PickledObjectField

from celery import conf
from celery import states
from celery.managers import TaskManager, TaskSetManager

TASK_STATUSES_CHOICES = zip(states.ALL_STATES, states.ALL_STATES)


class TaskMeta(models.Model):
    """Task result/status."""
    task_id = models.CharField(_(u"task id"), max_length=255, unique=True)
    status = models.CharField(_(u"task status"), max_length=50,
            default=states.PENDING, choices=TASK_STATUSES_CHOICES)
    result = PickledObjectField(null=True)
    date_done = models.DateTimeField(_(u"done at"), auto_now=True)
    traceback = models.TextField(_(u"traceback"), blank=True, null=True)

    objects = TaskManager()

    class Meta:
        """Model meta-data."""
        verbose_name = _(u"task meta")
        verbose_name_plural = _(u"task meta")

    def to_dict(self):
        return {"task_id": self.task_id,
                "status": self.status,
                "result": self.result,
                "date_done": self.date_done,
                "traceback": self.traceback}

    def __unicode__(self):
        return u"<Task: %s successful: %s>" % (self.task_id, self.status)


class TaskSetMeta(models.Model):
    """TaskSet result"""
    taskset_id = models.CharField(_(u"task id"), max_length=255, unique=True)
    result = PickledObjectField()
    date_done = models.DateTimeField(_(u"done at"), auto_now=True)

    objects = TaskSetManager()

    class Meta:
        """Model meta-data."""
        verbose_name = _(u"taskset meta")
        verbose_name_plural = _(u"taskset meta")

    def to_dict(self):
        return {"taskset_id": self.taskset_id,
                "result": self.result,
                "date_done": self.date_done}

    def __unicode__(self):
        return u"<TaskSet: %s>" % (self.taskset_id)

if (django.VERSION[0], django.VERSION[1]) >= (1, 1):
    # keep models away from syncdb/reset if database backend is not
    # being used.
    if conf.RESULT_BACKEND != 'database':
        TaskMeta._meta.managed = False
        TaskSetMeta._meta.managed = False
