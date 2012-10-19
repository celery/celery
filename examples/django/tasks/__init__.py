from __future__ import absolute_import

from celery import Celery
from django.conf import settings


celery = Celery('tasks', broker='amqp://localhost')
celery.config_from_object(settings)
celery.autodiscover_tasks(settings.INSTALLED_APPS)


@celery.task
def debug_task():
    print(repr(debug_task.request))
