from __future__ import absolute_import

import os

BROKER_TRANSPORT = "memory"

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
CELERY_REDIS_HOST = os.environ.get("REDIS_HOST") or "localhost"
CELERY_REDIS_PORT = int(os.environ.get("REDIS_PORT") or 6379)
CELERY_REDIS_DB = os.environ.get("REDIS_DB") or 0
CELERY_REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")

# Mongo results tests (only executed if installed and running)
CELERY_MONGODB_BACKEND_SETTINGS = {
    "host": os.environ.get("MONGO_HOST") or "localhost",
    "port": os.environ.get("MONGO_PORT") or 27017,
    "database": os.environ.get("MONGO_DB") or "celery_unittests",
    "taskmeta_collection": os.environ.get("MONGO_TASKMETA_COLLECTION") or
        "taskmeta_collection",
}
if os.environ.get("MONGO_USER"):
    CELERY_MONGODB_BACKEND_SETTINGS["user"] = os.environ.get("MONGO_USER")
if os.environ.get("MONGO_PASSWORD"):
    CELERY_MONGODB_BACKEND_SETTINGS["password"] = \
        os.environ.get("MONGO_PASSWORD")
