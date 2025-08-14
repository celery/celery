import json
import os
import re
import time

import pytest

# we have to import the pytest plugin fixtures here,
# in case user did not do the `python setup.py develop` yet,
# that installs the pytest plugin into the setuptools registry.
from celery.contrib.pytest import celery_app, celery_session_worker
from celery.contrib.testing.manager import Manager
from t.integration.tasks import get_redis_connection

TEST_BROKER = os.environ.get('TEST_BROKER', 'pyamqp://')
TEST_BACKEND = os.environ.get('TEST_BACKEND', 'redis://')

# Tricks flake8 into silencing redefining fixtures warnings.
__all__ = (
    'celery_app',
    'celery_session_worker',
    'get_active_redis_channels',
)


def get_active_redis_channels():
    return get_redis_connection().execute_command('PUBSUB CHANNELS')


def check_for_logs(
    caplog,
    message: str,
    max_wait: float = 1.0,
    interval: float = 0.1
) -> bool:
    """Check if a specific message exists in the logs.

    Args:
        caplog: The pytest caplog fixture
        message: The message to look for, can be a regex pattern
        max_wait: Maximum time to wait for the log message (in seconds)
        interval: Time between log checks (in seconds)

    Returns:
        bool: True if the message was found, False otherwise
    """
    start_time = time.monotonic()
    while time.monotonic() - start_time < max_wait:
        # Check if the message is in the logs
        if any(re.search(message, record.message) for record in caplog.records):
            return True
        time.sleep(interval)
    return False


@pytest.fixture(scope='session')
def celery_config(request):
    config = {
        'broker_url': TEST_BROKER,
        'result_backend': TEST_BACKEND,
        'cassandra_servers': ['localhost'],
        'cassandra_keyspace': 'tests',
        'cassandra_table': 'tests',
        'cassandra_read_consistency': 'ONE',
        'cassandra_write_consistency': 'ONE',
        'result_extended': True
    }
    try:
        # To override the default configuration, create the integration-tests-config.json file
        # in Celery's root directory.
        # The file must contain a dictionary of valid configuration name/value pairs.
        config_overrides = json.load(open(str(request.config.rootdir / "integration-tests-config.json")))
        config.update(config_overrides)
    except OSError:
        pass
    return config


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
    manager = Manager(app)
    yield manager
    manager.wait_until_idle()


@pytest.fixture(autouse=True)
def ZZZZ_set_app_current(app):
    app.set_current()
    app.set_default()


@pytest.fixture(scope='session')
def celery_class_tasks():
    from t.integration.tasks import ClassBasedAutoRetryTask
    return [ClassBasedAutoRetryTask]
