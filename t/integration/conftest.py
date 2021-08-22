import os

import pytest

# we have to import the pytest plugin fixtures here,
# in case user did not do the `python setup.py develop` yet,
# that installs the pytest plugin into the setuptools registry.
from celery.contrib.pytest import celery_app, celery_session_worker
from celery.contrib.testing.manager import Manager

TEST_BROKER = os.environ.get('TEST_BROKER', 'pyamqp://')
TEST_BACKEND = os.environ.get('TEST_BACKEND', 'redis://')

# Tricks flake8 into silencing redefining fixtures warnings.
__all__ = (
    'celery_app',
    'celery_session_worker',
    'get_active_redis_channels',
    'get_redis_connection',
)


def get_redis_connection():
    from redis import StrictRedis
    return StrictRedis(host=os.environ.get('REDIS_HOST'))


def get_active_redis_channels():
    return get_redis_connection().execute_command('PUBSUB CHANNELS')


@pytest.fixture(scope='session')
def celery_config():
    return {
        'broker_url': TEST_BROKER,
        'result_backend': TEST_BACKEND,
        'cassandra_servers': ['localhost'],
        'cassandra_keyspace': 'tests',
        'cassandra_table': 'tests',
        'cassandra_read_consistency': 'ONE',
        'cassandra_write_consistency': 'ONE'
    }


@pytest.fixture(scope='session')
def celery_enable_logging():
    return True


@pytest.fixture(scope='session')
def celery_worker_pool():
    return 'prefork'


@pytest.fixture(scope='session')
def celery_includes():
    return {'t.integration.tasks'}


@pytest.fixture
def app(celery_app):
    yield celery_app


@pytest.fixture
def manager(app, celery_session_worker):
    return Manager(app)


@pytest.fixture(autouse=True)
def ZZZZ_set_app_current(app):
    app.set_current()
    app.set_default()


@pytest.fixture(scope='session')
def celery_class_tasks():
    from t.integration.tasks import ClassBasedAutoRetryTask
    return [ClassBasedAutoRetryTask]
