import atexit
import os

BROKER_URL = os.environ.get('BROKER_URL') or 'amqp://'
CELERY_RESULT_BACKEND = 'amqp://'
CELERY_SEND_TASK_ERROR_EMAILS = False

CELERY_DEFAULT_QUEUE = 'testcelery'
CELERY_DEFAULT_EXCHANGE = 'testcelery'
CELERY_DEFAULT_ROUTING_KEY = 'testcelery'
CELERY_QUEUES = {'testcelery': {'routing_key': 'testcelery'}}

CELERYD_LOG_COLOR = False

CELERY_IMPORTS = ('celery.tests.functional.tasks', )


@atexit.register
def teardown_testdb():
    import os
    if os.path.exists('test.db'):
        os.remove('test.db')
