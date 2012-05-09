from __future__ import absolute_import

from celery import Celery

celery = Celery(broker="amqp://",
                backend="amqp://",
                include=["proj.tasks"])
