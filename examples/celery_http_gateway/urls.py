from __future__ import absolute_import, unicode_literals

from django.conf.urls.defaults import (  # noqa
    url, patterns, include, handler404, handler500,
)

from djcelery import views as celery_views

from celery_http_gateway.tasks import hello_world

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns(
    '',
    url(r'^apply/(?P<task_name>.+?)/', celery_views.apply),
    url(r'^hello/', celery_views.task_view(hello_world)),
    url(r'^(?P<task_id>[\w\d\-]+)/done/?$', celery_views.is_task_successful,
        name='celery-is_task_successful'),
    url(r'^(?P<task_id>[\w\d\-]+)/status/?$', celery_views.task_status,
        name='celery-task_status'),
)
