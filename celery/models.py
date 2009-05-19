from django.db import models
from celery.registry import tasks
from celery.managers import TaskManager, PeriodicTaskManager
from yadayada.models import PickledObjectField
from django.utils.translation import ugettext_lazy as _
from Queue import Queue


class RetryQueue(object):
    queue = Queue()

    class Item(object):
        def __init__(self, task_name, task_id, args, kwargs):
            self.task_name = task_name
            self.task_id = task_id
            self.args = args
            self.kwargs = kwargs
            self.retry_count = 0

        def retry(self):
            self.task.requeue(self.task_id, self.args, self.kwargs)
            self.retry_count += 1
            self.last_retry_at = time.time()
            return self.retry_count

        @property
        def task(self):
            return tasks[self.task_name]

    def put(self, task_name, task_id, args, kwargs):
        self.queue.put(self.Item(task_name, task_id, args, kwargs))

    def get(self):
        if not self.queue.qsize():
            return None
        return self.queue.get()
retry_queue = RetryQueue()


TASK_STATUS_PENDING = "PENDING"
TASK_STATUS_RETRY = "RETRY"
TASK_STATUS_FAILURE = "FAILURE"
TASK_STATUS_DONE = "DONE"
TASK_STATUSES = (TASK_STATUS_PENDING, TASK_STATUS_RETRY,
                 TASK_STATUS_FAILURE, TASK_STATUS_DONE)
TASK_STATUSES_CHOICES = zip(TASK_STATUSES, TASK_STATUSES)
                

class TaskMeta(models.Model):
    task_id = models.CharField(_(u"task id"), max_length=255, unique=True)
    status = models.CharField(_(u"task status"), max_length=50,
            default=TASK_STATUS_PENDING, choices=TASK_STATUSES_CHOICES)
    result = PickledObjectField()
    date_done = models.DateTimeField(_(u"done at"), auto_now=True)

    objects = TaskManager()

    class Meta:
        verbose_name = _(u"task meta")
        verbose_name_plural = _(u"task meta")

    def __unicode__(self):
        return u"<Task: %s done:%s>" % (self.task_id, self.status)


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

    def delay(self, *args, **kwargs):
        self.task.delay()
        self.total_run_count = self.total_run_count + 1
        self.save()

    @property
    def task(self):
        return tasks[self.name]
