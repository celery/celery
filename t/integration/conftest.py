import atexit
import json
import logging
import os
import re
import signal
import threading
import time

import psutil
import pytest

from celery.contrib.pytest import celery_app, celery_session_worker
from celery.contrib.testing.manager import Manager
from t.integration.tasks import get_redis_connection

logger = logging.getLogger(__name__)

TEST_BROKER = os.environ.get('TEST_BROKER', 'pyamqp://')
TEST_BACKEND = os.environ.get('TEST_BACKEND', 'redis://')

__all__ = (
    'celery_app',
    'celery_session_worker',
    'get_active_redis_channels',
)


def safe_join(thread: threading.Thread, timeout=10):
    if thread.is_alive():
        thread.join(timeout)
        if thread.is_alive():
            logger.warning("[safe_join] Worker thread did not stop after %ds, forcing shutdown.", timeout)


def pytest_sessionstart(session):
    import celery.contrib.testing.worker as worker_mod
    orig_start_worker = worker_mod._start_worker_thread

    def patched_start_worker_thread(*args, **kwargs):
        thread_holder = {}

        class WrappedContext:
            def __init__(self, orig_context):
                self._ctx = orig_context

            def __enter__(self):
                result = self._ctx.__enter__()
                thread = kwargs.get("thread") or getattr(self._ctx, "thread", None)
                thread_holder["thread"] = thread
                return result

            def __exit__(self, exc_type, exc_val, exc_tb):
                try:
                    return self._ctx.__exit__(exc_type, exc_val, exc_tb)
                finally:
                    thread = thread_holder.get("thread")
                    if thread:
                        safe_join(thread, timeout=10)

        ctx = orig_start_worker(*args, **kwargs)
        return WrappedContext(ctx)

    worker_mod._start_worker_thread = patched_start_worker_thread


def _broadcast_shutdown(app):
    try:
        logger.info("[sessionfinish] Broadcasting shutdown to all Celery workers")
        app.control.broadcast("shutdown")
    except Exception as e:
        logger.warning("Broadcast shutdown failed: %s", e)


def pytest_sessionfinish(session, exitstatus):
    """Ensure workers are cleanly shut down at the end of the session."""
    app = getattr(session.config, "celery_app", None)
    if app is not None:
        _broadcast_shutdown(app)
        return

    for item in getattr(session, "items", []) or []:
        funcargs = getattr(item, "funcargs", None)
        if funcargs and "manager" in funcargs:
            _broadcast_shutdown(funcargs["manager"].app)
            break


@atexit.register
def kill_orphaned_children():
    current = psutil.Process()
    children = current.children(recursive=True)
    for proc in children:
        try:
            logger.warning("[atexit] Killing orphaned child: PID %s", proc.pid)
            proc.send_signal(signal.SIGTERM)
        except Exception:
            pass


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
        'result_extended': True,
        'cassandra_servers': ['localhost'],
        'cassandra_keyspace': 'tests',
        'cassandra_table': 'tests',
        'cassandra_read_consistency': 'ONE',
        'cassandra_write_consistency': 'ONE',
    }
    try:
        overrides = json.load(open(str(request.config.rootdir / "integration-tests-config.json")))
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
        logger.info("Stopping Celery manager")
        manager.wait_until_idle()
        logger.info("Manager stopped cleanly")
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
