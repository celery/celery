from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest
from pytest_celery import CeleryTestSetup, CeleryTestWorker, CeleryWorkerCluster

from celery import Celery
from celery.app.base import set_default_app
from celery.signals import after_task_publish
from t.integration.tasks import identity


@pytest.fixture(
    params=[
        # Single worker
        ["celery_setup_worker"],
        # Workers cluster (same queue)
        ["celery_setup_worker", "celery_alt_dev_worker"],
    ]
)
def celery_worker_cluster(request: pytest.FixtureRequest) -> CeleryWorkerCluster:
    nodes: tuple[CeleryTestWorker] = [
        request.getfixturevalue(worker) for worker in request.param
    ]
    cluster = CeleryWorkerCluster(*nodes)
    yield cluster
    cluster.teardown()


class test_thread_safety:
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.broker_pool_limit = 42
        return app

    @pytest.mark.parametrize(
        "threads_count",
        [
            # Single
            1,
            # Multiple
            2,
            # Many
            42,
        ],
    )
    def test_multithread_task_publish(
        self,
        celery_setup: CeleryTestSetup,
        threads_count: int,
    ):
        signal_was_called = Mock()

        @after_task_publish.connect
        def after_task_publish_handler(*args, **kwargs):
            nonlocal signal_was_called
            signal_was_called(True)

        def thread_worker():
            set_default_app(celery_setup.app)
            identity.si("Published from thread").apply_async(
                queue=celery_setup.worker.worker_queue
            )

        executor = ThreadPoolExecutor(threads_count)

        with executor:
            for _ in range(threads_count):
                executor.submit(thread_worker)

        assert signal_was_called.call_count == threads_count
