import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup, RedisTestBroker

from celery import Celery
from celery.canvas import group
from t.smoke.tasks import long_running_task, noop

WORKER_PREFETCH_MULTIPLIER = 2
WORKER_CONCURRENCY = 5
MAX_PREFETCH = WORKER_PREFETCH_MULTIPLIER * WORKER_CONCURRENCY


@pytest.fixture
def default_worker_app(default_worker_app: Celery) -> Celery:
    app = default_worker_app
    app.conf.worker_prefetch_multiplier = WORKER_PREFETCH_MULTIPLIER
    app.conf.worker_concurrency = WORKER_CONCURRENCY
    yield app


class test_worker_enable_prefetch_count_reduction_true:
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.worker_enable_prefetch_count_reduction = True
        yield app

    @pytest.mark.parametrize("expected_running_tasks_count", range(1, WORKER_CONCURRENCY + 1))
    def test_reducing_prefetch_count(self, celery_setup: CeleryTestSetup, expected_running_tasks_count: int):
        if isinstance(celery_setup.broker, RedisTestBroker):
            pytest.xfail("Potential Bug: Redis Broker Restart is unstable")

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
        celery_setup.worker.assert_log_exists(expected_prefetch_reduce_message)

        expected_prefetch_restore_message = (
            f"The prefetch count will be gradually restored to {MAX_PREFETCH} as the tasks complete processing."
        )
        celery_setup.worker.assert_log_exists(expected_prefetch_restore_message)

    def test_prefetch_count_restored(self, celery_setup: CeleryTestSetup):
        if isinstance(celery_setup.broker, RedisTestBroker):
            pytest.xfail("Potential Bug: Redis Broker Restart is unstable")

        expected_running_tasks_count = MAX_PREFETCH * WORKER_PREFETCH_MULTIPLIER
        sig = group(long_running_task.s(10) for _ in range(expected_running_tasks_count))
        sig.apply_async(queue=celery_setup.worker.worker_queue)
        celery_setup.broker.restart()
        expected_prefetch_restore_message = (
            f"Resuming normal operations following a restart.\n"
            f"Prefetch count has been restored to the maximum of {MAX_PREFETCH}"
        )
        celery_setup.worker.assert_log_exists(expected_prefetch_restore_message)

    class test_cancel_tasks_on_connection_loss:
        @pytest.fixture
        def default_worker_app(self, default_worker_app: Celery) -> Celery:
            app = default_worker_app
            app.conf.worker_prefetch_multiplier = 2
            app.conf.worker_cancel_long_running_tasks_on_connection_loss = True
            app.conf.task_acks_late = True
            yield app

        def test_max_prefetch_passed_on_broker_restart(self, celery_setup: CeleryTestSetup):
            if isinstance(celery_setup.broker, RedisTestBroker):
                pytest.xfail("Real Bug: Broker does not fetch messages after restart")

            sig = group(long_running_task.s(420) for _ in range(WORKER_CONCURRENCY))
            sig.apply_async(queue=celery_setup.worker.worker_queue)
            celery_setup.broker.restart()
            noop.s().apply_async(queue=celery_setup.worker.worker_queue)
            celery_setup.worker.assert_log_exists("Task t.smoke.tasks.noop")


class test_worker_enable_prefetch_count_reduction_false:
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.worker_prefetch_multiplier = 1
        app.conf.worker_enable_prefetch_count_reduction = False
        app.conf.worker_cancel_long_running_tasks_on_connection_loss = True
        app.conf.task_acks_late = True
        yield app

    def test_max_prefetch_not_passed_on_broker_restart(self, celery_setup: CeleryTestSetup):
        if isinstance(celery_setup.broker, RedisTestBroker):
            pytest.xfail("Real Bug: Broker does not fetch messages after restart")

        sig = group(long_running_task.s(10) for _ in range(WORKER_CONCURRENCY))
        r = sig.apply_async(queue=celery_setup.worker.worker_queue)
        celery_setup.broker.restart()
        noop.s().apply_async(queue=celery_setup.worker.worker_queue)
        assert "Task t.smoke.tasks.noop" not in celery_setup.worker.logs()
        r.get(timeout=RESULT_TIMEOUT)
        assert "Task t.smoke.tasks.noop" in celery_setup.worker.logs()
