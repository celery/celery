import json
import logging
import os
import re
import time

import pytest

from celery.contrib.pytest import celery_app, celery_session_worker
from celery.contrib.testing.manager import Manager
from celery.exceptions import TimeoutError
from t.integration.tasks import get_redis_connection

# we have to import the pytest plugin fixtures here,
# in case user did not do the `python setup.py develop` yet,
# that installs the pytest plugin into the setuptools registry.


logger = logging.getLogger(__name__)

TEST_BROKER = os.environ.get('TEST_BROKER', 'pyamqp://')
TEST_BACKEND = os.environ.get('TEST_BACKEND', 'redis://')

RETRYABLE_EXCEPTIONS = (OSError, ConnectionError, TimeoutError)


def is_retryable_exception(exc):
    return isinstance(exc, RETRYABLE_EXCEPTIONS)


_flaky = pytest.mark.flaky(reruns=5, reruns_delay=1, cause=is_retryable_exception)
_timeout = pytest.mark.timeout(timeout=300)


def flaky(fn):
    return _timeout(_flaky(fn))


__all__ = (
    'celery_app',
    'celery_session_worker',
    'flaky',
    'get_active_redis_channels',
)


def pytest_collection_modifyitems(config, items):
    """Guard: integration tests must not call start_worker().

    Integration tests share a session-scoped worker from conftest.py.
    Tests that need their own worker belong in t/smoke/tests/.
    """
    integration_dir = str(config.rootdir / 't' / 'integration')
    checked = set()
    for item in items:
        path = str(item.path)
        if path in checked:
            continue
        checked.add(path)
        if not path.startswith(integration_dir):
            continue
        if not item.path.name.startswith('test_'):
            continue
        content = item.path.read_text('utf-8')
        if re.search(r'\bstart_worker\(', content):
            pytest.exit(
                f"\n{item.path.name} calls start_worker().\n"
                f"Integration tests use the session-scoped worker from conftest.py.\n"
                f"Tests requiring custom workers belong in t/smoke/tests/.\n",
                returncode=1,
            )


def get_active_redis_channels():
    return get_redis_connection().execute_command('PUBSUB CHANNELS')


def check_for_logs(caplog, message: str, max_wait: float = 1.0, interval: float = 0.1) -> bool:
    start_time = time.monotonic()
    while time.monotonic() - start_time < max_wait:
        if any(re.search(message, record.message) for record in caplog.records):
            return True
        time.sleep(interval)
    return False


@pytest.fixture(scope='session')
def celery_config(request):
    config = {
        'broker_url': TEST_BROKER,
        'result_backend': TEST_BACKEND,
        'task_create_missing_queues': False,
        'result_extended': True,
        'cassandra_servers': ['localhost'],
        'cassandra_keyspace': 'tests',
        'cassandra_table': 'tests',
        'cassandra_read_consistency': 'ONE',
        'cassandra_write_consistency': 'ONE',
    }
    try:
        # To override the default configuration, create the integration-tests-config.json file
        # in Celery's root directory.
        # The file must contain a dictionary of valid configuration name/value pairs.
        with open(str(request.config.rootdir / "integration-tests-config.json")) as file:
            overrides = json.load(file)
        config.update(overrides)
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
    try:
        manager.wait_until_idle()
    except Exception as e:
        logger.warning("Failed to stop Celery test manager cleanly: %s", e)


@pytest.fixture(autouse=True)
def ZZZZ_set_app_current(app):
    app.set_current()
    app.set_default()


@pytest.fixture(scope='session')
def celery_class_tasks():
    from t.integration.tasks import ClassBasedAutoRetryTask
    return [ClassBasedAutoRetryTask]


@pytest.fixture(autouse=True, scope='module')
def _assert_no_queue_pollution(celery_session_app):
    """Integration tests must not register new queues.

    The session worker consumes from the default queue only.
    task_create_missing_queues=False prevents accidental creation,
    and this fixture verifies the invariant holds.
    """
    before = set(celery_session_app.amqp.queues)
    yield
    leaked = set(celery_session_app.amqp.queues) - before
    if leaked:
        pytest.fail(
            f"Module registered unexpected queues: {sorted(leaked)}\n"
            f"Either clean up in teardown, or move to t/smoke/tests/."
        )
