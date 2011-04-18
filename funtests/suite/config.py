import atexit
import os

BROKER_TRANSPORT = (os.environ.get("BROKER_TRANSPORT") or
                    os.environ.get("BROKER_BACKEND") or "amqplib")

BROKER_HOST = os.environ.get("BROKER_HOST") or "localhost"
BROKER_USER = os.environ.get("BROKER_USER") or "guest"
BROKER_PASSWORD = os.environ.get("BROKER_PASSWORD") or "guest"
BROKER_VHOST = os.environ.get("BROKER_VHOST") or "/"


CELERY_RESULT_BACKEND = "amqp"
CELERY_SEND_TASK_ERROR_EMAILS = False

CELERY_DEFAULT_QUEUE = "testcelery"
CELERY_DEFAULT_EXCHANGE = "testcelery"
CELERY_DEFAULT_ROUTING_KEY = "testcelery"
CELERY_QUEUES = {"testcelery": {"binding_key": "testcelery"}}

CELERYD_LOG_COLOR = False

CELERY_IMPORTS = ("celery.tests.functional.tasks", )


@atexit.register
def teardown_testdb():
    import os
    if os.path.exists("test.db"):
        os.remove("test.db")
