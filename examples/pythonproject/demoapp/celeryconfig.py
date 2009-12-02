import os
import sys
sys.path.insert(0, os.getcwd())

DATABASE_ENGINE = "sqlite3"
DATABASE_NAME = "celery.db"
BROKER_HOST = "localhost"
BROKER_USER = "guest"
BROKER_PASSWORD = "guest"
BROKER_VHOST = "/"
CELERY_BACKEND = "amqp"
CELERY_IMPORTS = ("tasks", )
