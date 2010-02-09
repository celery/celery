import os
import sys
sys.path.insert(0, os.getcwd())

DATABASE_ENGINE = "sqlite3"
DATABASE_NAME = "celery.db"

# or for Redis use ghettoq.taproot.Redis
CARROT_BACKEND = "ghettoq.taproot.Database"

# not needed for database.
# BROKER_HOST = "localhost"
# BROKER_USER = "guest"
# BROKER_PASSWORD = "guest"
# BROKER_VHOST = "/"

# Need to add ghettoq when using the database backend, so
# the database tables are created at syncdb.
INSTALLED_APPS = ("celery", "ghettoq")

CELERY_IMPORTS = ("tasks", )
