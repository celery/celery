import sys
import os
sys.path.insert(0, os.getcwd())

BROKER_URL = "amqp://guest:guest@localhost:5672//"

CELERY_IMPORTS = ("tasks", )

## Using the database to store results
# CELERY_RESULT_BACKEND = "database"
# CELERY_RESULT_DBURI = "sqlite:///celerydb.sqlite"

# Results published as messages (requires AMQP).
CELERY_RESULT_BACKEND = "amqp"
