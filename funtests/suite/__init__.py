from __future__ import absolute_import, unicode_literals

import os
import sys

sys.path.insert(0, os.getcwd())
sys.path.insert(0, os.path.join(os.getcwd(), os.pardir))

config = os.environ.setdefault('CELERY_FUNTEST_CONFIG_MODULE',
                               'suite.config')

os.environ['CELERY_CONFIG_MODULE'] = config
os.environ['CELERY_LOADER'] = 'default'
