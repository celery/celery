from __future__ import absolute_import, unicode_literals

import atexit
import os

broker_url = os.environ.get('BROKER_URL') or 'amqp://'
result_backend = 'amqp://'
send_task_error_emails = False

default_queue = 'testcelery'
default_exchange = 'testcelery'
default_routing_key = 'testcelery'
queues = {'testcelery': {'routing_key': 'testcelery'}}

log_color = False

imports = ('celery.tests.functional.tasks',)


@atexit.register
def teardown_testdb():
    import os
    if os.path.exists('test.db'):
        os.remove('test.db')
