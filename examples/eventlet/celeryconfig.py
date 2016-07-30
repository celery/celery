from __future__ import absolute_import, unicode_literals

import os
import sys
sys.path.insert(0, os.getcwd())

# ## Start worker with -P eventlet
# Never use the worker_pool setting as that'll patch
# the worker too late.

broker_url = 'amqp://guest:guest@localhost:5672//'
worker_disable_rate_limits = True
result_backend = 'amqp'
result_expires = 30 * 60

imports = ('tasks', 'webcrawler')
