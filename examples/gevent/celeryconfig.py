from __future__ import absolute_import, unicode_literals
import os
import sys
sys.path.insert(0, os.getcwd())

# ## Note: Start worker with -P gevent,
# do not use the worker_pool option.

broker_url = 'amqp://guest:guest@localhost:5672//'
result_backend = 'amqp'
result_expires = 30 * 60

imports = ('tasks',)
