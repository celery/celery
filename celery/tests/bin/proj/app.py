from __future__ import absolute_import, unicode_literals

from celery import Celery

app = Celery(set_as_current=False)
