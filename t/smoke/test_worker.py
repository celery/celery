import pytest
from pytest_celery import CeleryTestSetup

from celery import Celery
from t.smoke.tasks import long_running_task

WORKER_PREFETCH_MULTIPLIER = 1
WORKER_CONCURRENCY = 3


@pytest.fixture
def default_worker_app(default_worker_app: Celery) -> Celery:
    app = default_worker_app
    app.conf.worker_prefetch_multiplier = WORKER_PREFETCH_MULTIPLIER
    app.conf.worker_concurrency = WORKER_CONCURRENCY
    yield app


class test_consumer:
    def test_reducing_prefetch_count(self, celery_setup: CeleryTestSetup):
        worker_prefetch_multiplier = celery_setup.app.conf.worker_prefetch_multiplier
        worker_concurrency = celery_setup.app.conf.worker_concurrency

        assert worker_prefetch_multiplier == WORKER_PREFETCH_MULTIPLIER
        assert worker_concurrency == WORKER_CONCURRENCY

        long_running_task.s().apply_async(queue=celery_setup.worker.worker_queue)
        celery_setup.broker.restart()

        prefetch_reduce_message = (
            f"Temporarily reducing the prefetch count to {worker_concurrency-worker_prefetch_multiplier} "
            f"to avoid over-fetching since 1 tasks are currently being processed."
        )
        celery_setup.worker.wait_for_log(prefetch_reduce_message)

        prefetch_restore_message = (
            f"The prefetch count will be gradually restored to {worker_concurrency} "
            f"as the tasks complete processing."
        )
        celery_setup.worker.wait_for_log(prefetch_restore_message)
