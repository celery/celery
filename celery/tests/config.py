import os

BROKER_BACKEND = "memory"

#: Don't want log output when running suite.
CELERYD_HIJACK_ROOT_LOGGER = False

CELERY_RESULT_BACKEND = "cache"
CELERY_CACHE_BACKEND = "memory"
CELERY_RESULT_DBURI = "sqlite:///test.db"
CELERY_SEND_TASK_ERROR_EMAILS = False

CELERY_DEFAULT_QUEUE = "testcelery"
CELERY_DEFAULT_EXCHANGE = "testcelery"
CELERY_DEFAULT_ROUTING_KEY = "testcelery"
CELERY_QUEUES = {"testcelery": {"binding_key": "testcelery"}}

CELERYD_LOG_COLOR = False

# Tyrant results tests (only executed if installed and running)
TT_HOST = os.environ.get("TT_HOST") or "localhost"
TT_PORT = int(os.environ.get("TT_PORT") or 1978)

# Redis results tests (only executed if installed and running)
REDIS_HOST = os.environ.get("REDIS_HOST") or "localhost"
REDIS_PORT = int(os.environ.get("REDIS_PORT") or 6379)
REDIS_DB = os.environ.get("REDIS_DB") or 0
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")
