from __future__ import absolute_import, unicode_literals

import atexit
import os

broker_url = os.environ.get('BROKER_URL') or 'amqp://'
result_backend = 'amqp://'

default_queue = 'testcelery'
default_exchange = 'testcelery'
default_routing_key = 'testcelery'
queues = {'testcelery': {'routing_key': 'testcelery'}}

log_color = False


@atexit.register
def teardown_testdb():
    import os
    if os.path.exists('test.db'):
        os.remove('test.db')
