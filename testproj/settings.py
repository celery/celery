# Django settings for testproj project.

import os
import sys
# import source code dir
sys.path.insert(0, os.path.join(os.getcwd(), os.pardir))

DEBUG = True
TEMPLATE_DEBUG = DEBUG

ADMINS = (
    # ('Your Name', 'your_email@domain.com'),
)

TEST_RUNNER = "celery.tests.runners.run_tests"
TEST_APPS = (
    "celery",
)

AMQP_SERVER = "localhost"
AMQP_PORT = 5672
AMQP_VHOST = "/"
AMQP_USER = "guest"
AMQP_PASSWORD = "guest"

CELERY_AMQP_EXCHANGE = "testcelery"
CELERY_AMQP_ROUTING_KEY = "testcelery"
CELERY_AMQP_CONSUMER_QUEUE = "testcelery"

CELERY_TASK_META_USE_DB = True

MANAGERS = ADMINS

DATABASE_ENGINE = 'sqlite3'
DATABASE_NAME = 'testdb.sqlite'
DATABASE_USER = ''
DATABASE_PASSWORD = ''
DATABASE_HOST = ''
DATABASE_PORT = ''

INSTALLED_APPS = (
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'celery',
    'someapp',
    'someappwotask',
)

try:
    import test_extensions
except ImportError:
    pass
else:
    INSTALLED_APPS += ("test_extensions", )
