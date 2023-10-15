import pytest
from pytest_celery import CeleryTestSetup

from celery import Celery
from t.smoke.tasks import long_running_task

WORKER_PREFETCH_MULTIPLIER = 2
WORKER_CONCURRENCY = 3
MAX_PREFETCH = WORKER_PREFETCH_MULTIPLIER * WORKER_CONCURRENCY


@pytest.fixture
def default_worker_app(default_worker_app: Celery) -> Celery:
    app = default_worker_app
    app.conf.worker_prefetch_multiplier = WORKER_PREFETCH_MULTIPLIER
    app.conf.worker_concurrency = WORKER_CONCURRENCY
    yield app


class test_consumer:
    def test_reducing_prefetch_count(self, celery_setup: CeleryTestSetup):
        assert celery_setup.app.conf.worker_prefetch_multiplier == WORKER_PREFETCH_MULTIPLIER
        assert celery_setup.app.conf.worker_concurrency == WORKER_CONCURRENCY

        long_running_task.s().apply_async(queue=celery_setup.worker.worker_queue)
        celery_setup.broker.restart()

        expected_running_tasks_count = 1
        expected_reduced_prefetch = (WORKER_PREFETCH_MULTIPLIER * WORKER_CONCURRENCY) - (
            WORKER_PREFETCH_MULTIPLIER * expected_running_tasks_count
        )

        expected_prefetch_reduce_message = (
            f"Temporarily reducing the prefetch count to {expected_reduced_prefetch} "
            f"to avoid over-fetching since {expected_running_tasks_count} tasks are currently being processed."
        )
        celery_setup.worker.wait_for_log(expected_prefetch_reduce_message)

        expected_prefetch_restore_message = (
            f"The prefetch count will be gradually restored to {MAX_PREFETCH} " f"as the tasks complete processing."
        )
        celery_setup.worker.wait_for_log(expected_prefetch_restore_message)
