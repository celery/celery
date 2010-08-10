CARROT_BACKEND = "memory"


CELERY_RESULT_BACKEND = "database"
CELERY_RESULT_DBURI = "sqlite:///test.db"
CELERY_SEND_TASK_ERROR_EMAILS = False

CELERY_DEFAULT_QUEUE = "testcelery"
CELERY_DEFAULT_EXCHANGE = "testcelery"
CELERY_DEFAULT_ROUTING_KEY = "testcelery"
CELERY_QUEUES = {"testcelery": {"binding_key": "testcelery"}}

CELERYD_LOG_COLOR = False
