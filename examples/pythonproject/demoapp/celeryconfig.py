import os
import sys
sys.path.insert(0, os.getcwd())

DATABASE_ENGINE = "sqlite3"
DATABASE_NAME = "celery.db"
BROKER_HOST = "localhost"
BROKER_USER = "guest"
BROKER_PASSWORD = "guest"
BROKER_VHOST = "celery"
CELERY_DEFAULT_EXCHANGE = "celery"
CARROT_BACKEND = "ghettoq.taproot.Redis"
CELERY_BACKEND = "database"
CELERY_IMPORTS = ("tasks", )
