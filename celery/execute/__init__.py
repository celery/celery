from __future__ import absolute_import

from celery import current_app
from celery.local import Proxy

send_task = Proxy(lambda: current_app.send_task)
