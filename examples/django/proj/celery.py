from __future__ import absolute_import

from celery import Celery
from django.conf import settings

app = Celery('proj.celery')

# Using a string here means the worker will not have to
# pickle the object when using Windows.
app.config_from_object('django.conf:settings')
app.autodiscover_tasks(settings.INSTALLED_APPS)


@app.task(bind=True)
def debug_task(self):
    print(repr(self.request))
