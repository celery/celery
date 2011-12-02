import os
import sys
sys.path.insert(0, os.getcwd())

CELERYD_POOL = "gevent"

BROKER_URL = "amqp://guest:guest@localhost:5672//"
CELERY_DISABLE_RATE_LIMITS = True
CELERY_RESULT_BACKEND = "amqp"
CELERY_TASK_RESULT_EXPIRES = 30 * 60

CELERY_IMPORTS = ("tasks", )
