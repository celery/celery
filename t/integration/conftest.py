from __future__ import absolute_import, unicode_literals
import os
import time
import pytest
from functools import wraps
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


@pytest.fixture(scope='session')
def celery_config():
    return {
        'broker_url': TEST_BROKER,
        'result_backend': TEST_BACKEND,
        'task_default_queue': 'queue_{}'.format(int(time.time() * 1000))
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


@pytest.fixture
def uses_sqs_transport(app):
    return app.conf.broker_url.startswith('sqs://')


@pytest.fixture(autouse=True, scope='session')
def enable_boto3_logging():
    if celery_config()['broker_url'].startswith('sqs://'):
        import logging
        logging.getLogger('boto3').setLevel(logging.INFO)
        logging.getLogger('botocore').setLevel(logging.INFO)


@pytest.fixture(autouse=True, scope='function')
def redis_backend_cleanup():
    yield
    if celery_config()['result_backend'].startswith('redis://'):
        import redis
        redis.StrictRedis().flushall()


@pytest.fixture(autouse=True)
def ZZZZ_set_app_current(app):
    app.set_current()
    app.set_default()
