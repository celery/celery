import atexit

BROKER_HOST = "localhost"
BROKER_PORT = 5672
BROKER_USER = "guest"
BROKER_PASSWORD = "guest"
BROKER_VHOST = "/"

CELERY_RESULT_BACKEND = "database"
CELERY_RESULT_DBURI = "sqlite:///test.db"
CELERY_SEND_TASK_ERROR_EMAILS = False

@atexit.register
def teardown_testdb():
    import os
    if os.path.exists("test.db"):
        os.remove("test.db")
