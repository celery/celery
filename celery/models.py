"""

Django Models.

"""
import django
from django.db import models
from django.utils.translation import ugettext_lazy as _
from picklefield.fields import PickledObjectField

from celery import conf
from celery.managers import TaskManager

TASK_STATUS_PENDING = "PENDING"
TASK_STATUS_RETRY = "RETRY"
TASK_STATUS_FAILURE = "FAILURE"
TASK_STATUS_SUCCESS = "SUCCESS"
TASK_STATUSES = (TASK_STATUS_PENDING, TASK_STATUS_RETRY,
                 TASK_STATUS_FAILURE, TASK_STATUS_SUCCESS)
TASK_STATUSES_CHOICES = zip(TASK_STATUSES, TASK_STATUSES)


class TaskMeta(models.Model):
    """Task result/status."""
    task_id = models.CharField(_(u"task id"), max_length=255, unique=True)
    status = models.CharField(_(u"task status"), max_length=50,
            default=TASK_STATUS_PENDING, choices=TASK_STATUSES_CHOICES)
    result = PickledObjectField()
    date_done = models.DateTimeField(_(u"done at"), auto_now=True)
    traceback = models.TextField(_(u"traceback"), blank=True, null=True)

    objects = TaskManager()

    class Meta:
        """Model meta-data."""
        verbose_name = _(u"task meta")
        verbose_name_plural = _(u"task meta")

    def __unicode__(self):
        return u"<Task: %s done:%s>" % (self.task_id, self.status)

if (django.VERSION[0], django.VERSION[1]) >= (1, 1):
    # keep models away from syncdb/reset if database backend is not
    # being used.
    if conf.CELERY_BACKEND != 'database':
        TaskMeta._meta.managed = False
