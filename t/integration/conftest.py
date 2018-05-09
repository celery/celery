from __future__ import absolute_import, unicode_literals

import os
from functools import wraps

import pytest

from celery.contrib.testing.manager import Manager

TEST_BROKER = os.environ.get('TEST_BROKER', 'pyamqp://')
TEST_BACKEND = os.environ.get('TEST_BACKEND', 'redis://')


def flaky(fun):
    @wraps(fun)
    def _inner(*args, **kwargs):
        for i in reversed(range(3)):
            try:
                return fun(*args, **kwargs)
            except Exception:
                if not i:
                    raise
    _inner.__wrapped__ = fun
    return _inner


def get_redis_connection():
    from redis import StrictRedis
    return StrictRedis(host=os.environ.get('REDIS_HOST'))


def get_active_redis_channels():
    return get_redis_connection().execute_command('PUBSUB CHANNELS')


@pytest.fixture(scope='session')
def celery_config():
    return {
        'broker_url': TEST_BROKER,
        'result_backend': TEST_BACKEND
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
