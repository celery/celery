"""

URLs defined for celery.

"""
from django.conf.urls.defaults import patterns, url

from celery import views


urlpatterns = patterns("",
    url(r'^(?P<task_id>[\w\d\-]+)/done/?$', views.is_task_successful,
        name="celery-is_task_successful"),
    url(r'^(?P<task_id>[\w\d\-]+)/status/?$', views.task_status,
        name="celery-task_status"),
)
