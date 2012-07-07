from __future__ import absolute_import

from celery import Celery

celery = Celery(broker='amqp://',
                backend='amqp://',
                include=['proj.tasks'])

# Optional configuration, see the application user guide.
celery.conf.update(
    CELERY_TASK_RESULT_EXPIRES=3600,
)
