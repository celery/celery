import sys
import os
sys.path.insert(0, os.getcwd())

BROKER_HOST = "localhost"
BROKER_USER = "guest"
BROKER_PASSWORD = "guest"
BROKER_VHOST = "/"

CELERY_IMPORTS = ("tasks", )

## Using the database to store results
# CELERY_RESULT_BACKEND = "database"
# CELERY_RESULT_DBURI = "sqlite:///celerydb.sqlite"

# Results published as messages (requires AMQP).
CELERY_RESULT_BACKEND = "amqp"
