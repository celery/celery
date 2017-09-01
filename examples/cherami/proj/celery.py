from __future__ import absolute_import, unicode_literals
from celery import Celery

from cherami import consumer
from cherami import publisher


app = Celery('proj',
             broker='cherami://',
             include=['proj.tasks'],
             cherami_publisher=publisher,
             cherami_consumer=consumer,)

if __name__ == '__main__':
    app.start()
