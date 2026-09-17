import gc
import os

from celery.contrib.testing.app import TestApp, setup_default_app
from celery.contrib.testing.worker import start_worker

from .conftest import TEST_BACKEND


def open_fd_count():
    return len(os.listdir('/dev/fd'))


def run_one_fixture_cycle():
    app = TestApp(config={'result_backend': TEST_BACKEND})

    @app.task
    def store_result():
        return 'stored'

    with setup_default_app(app):
        with start_worker(app):
            # The task ensures the connection to the backend is opened.
            result = store_result.delay()
            assert result.get(timeout=10) == 'stored'
    return app


def test_fixture_teardown_releases_backend_connections():
    iterations = 20

    # Warm up imports and logging.
    apps = [run_one_fixture_cycle()]
    baseline = open_fd_count()

    for _ in range(iterations):
        apps.append(run_one_fixture_cycle())

    # Simulate automatic garbage collection.
    gc.collect()
    growth = open_fd_count() - baseline
    assert growth <= 0
