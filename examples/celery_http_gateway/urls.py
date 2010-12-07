from django.conf.urls.defaults import *

from celery.task import PingTask
from djcelery import views as celery_views

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns("",
    url(r'^apply/(?P<task_name>.+?)/', celery_views.apply),
    url(r'^ping/', celery_views.task_view(PingTask)),
    url(r'^(?P<task_id>[\w\d\-]+)/done/?$', celery_views.is_task_successful,
        name="celery-is_task_successful"),
    url(r'^(?P<task_id>[\w\d\-]+)/status/?$', celery_views.task_status,
        name="celery-task_status"),
)
