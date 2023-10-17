import pytest
from pytest_celery import CeleryTestSetup, RedisTestBroker

from celery import Celery
from celery.canvas import group
from t.smoke.tasks import long_running_task

WORKER_PREFETCH_MULTIPLIER = 2
WORKER_CONCURRENCY = 5
MAX_PREFETCH = WORKER_PREFETCH_MULTIPLIER * WORKER_CONCURRENCY


@pytest.fixture
def default_worker_app(default_worker_app: Celery) -> Celery:
    app = default_worker_app
    app.conf.worker_prefetch_multiplier = WORKER_PREFETCH_MULTIPLIER
    app.conf.worker_concurrency = WORKER_CONCURRENCY
    yield app


class test_consumer:
    @pytest.mark.parametrize("expected_running_tasks_count", range(1, WORKER_CONCURRENCY + 1))
    def test_reducing_prefetch_count(self, celery_setup: CeleryTestSetup, expected_running_tasks_count: int):
        sig = group(long_running_task.s(420) for _ in range(expected_running_tasks_count))
        sig.apply_async(queue=celery_setup.worker.worker_queue)
        celery_setup.broker.restart()

        expected_reduced_prefetch = max(
            WORKER_PREFETCH_MULTIPLIER, MAX_PREFETCH - expected_running_tasks_count * WORKER_PREFETCH_MULTIPLIER
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

    def test_prefetch_count_restored(self, celery_setup: CeleryTestSetup):
        if isinstance(celery_setup.broker, RedisTestBroker):
            pytest.xfail("Real bug in Redis broker")

        expected_running_tasks_count = MAX_PREFETCH+1
        sig = group(long_running_task.s(10) for _ in range(expected_running_tasks_count))
        sig.apply_async(queue=celery_setup.worker.worker_queue)
        celery_setup.broker.restart()
        expected_prefetch_restore_message = (
            f"Resuming normal operations following a restart.\n"
            f"Prefetch count has been restored to the maximum of {MAX_PREFETCH}"
        )
        celery_setup.worker.wait_for_log(expected_prefetch_restore_message)
