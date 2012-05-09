from __future__ import absolute_import

from celery import Celery

celery = Celery("proj", broker="amqp://", backend="amqp")
celery.conf.CELERY_IMPORTS = ("proj.tasks", )

if __name__ == "__main__":
    from billiard import freeze_support
    freeze_support()
    celery.start()
