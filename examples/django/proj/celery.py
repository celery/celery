from __future__ import absolute_import

from celery import Celery
from django.conf import settings


celery = Celery('tasks', broker='amqp://localhost')
celery.config_from_object(settings)
celery.autodiscover_tasks(settings.INSTALLED_APPS)


@celery.task(bind=True)
def debug_task(self):
    print(repr(self.request))
