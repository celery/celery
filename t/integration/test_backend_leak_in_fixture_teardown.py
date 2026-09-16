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

    # Teardown should close the backend connections before leaving.
    with setup_default_app(app):
        with start_worker(app):
            # The task ensures the connection to the backend is opened.
            result = store_result.delay()
            assert result.get(timeout=10) == 'stored'


def test_fixture_teardown_releases_backend_connections():
    iterations = 10

    # Automatic collections could hide a missing teardown collect, so
    # run the loop without them.
    gc.disable()
    try:
        # Warm-up cycle so lazy imports and logging setup do not count
        # against the baseline.
        run_one_fixture_cycle()
        gc.collect()
        baseline = open_fd_count()

        for _ in range(iterations):
            run_one_fixture_cycle()

        growth = open_fd_count() - baseline
    finally:
        gc.enable()
        gc.collect()

    # Each leaked backend keeps at least one redis socket open
    # (https://github.com/celery/celery/issues/6382), so a regression
    # grows the count by >= iterations; a healthy teardown keeps it flat.
    assert growth < iterations
