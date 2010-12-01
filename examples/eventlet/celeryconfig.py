import os
import sys
sys.path.insert(0, os.getcwd())

CELERYD_POOL = "eventlet"

BROKER_HOST = "localhost"
BROKER_USER = "guest"
BROKER_PASSWORD = "guest"
BROKER_VHOST = "/"
CELERY_DISABLE_RATE_LIMITS = True
CELERY_RESULT_BACKEND = "amqp"
CELERY_TASK_RESULT_EXPIRES = 30 * 60

CELERY_IMPORTS = ("tasks", "webcrawler")
