from __future__ import annotations

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup, CeleryTestWorker, CeleryWorkerCluster, RedisTestBroker

from celery import Celery
from t.smoke.tasks import long_running_task
from t.smoke.tests.conftest import WorkerOperations, WorkerTermination


@pytest.fixture
def celery_worker_cluster(
    celery_worker: CeleryTestWorker,
    celery_alt_dev_worker: CeleryTestWorker,
) -> CeleryWorkerCluster:
    cluster = CeleryWorkerCluster(celery_worker, celery_alt_dev_worker)
    yield cluster
    cluster.teardown()


@pytest.mark.parametrize("termination_method", list(WorkerTermination.Methods))
class test_worker_failover(WorkerOperations):
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.task_acks_late = True
        app.conf.worker_max_memory_per_child = 10 * 1024  # Limit to 10MB
        if app.conf.broker_url.startswith("redis"):
            app.conf.broker_transport_options = {"visibility_timeout": 1}
        yield app

    def test_killing_first_worker(
        self,
        celery_setup: CeleryTestSetup,
        termination_method: WorkerTermination,
    ):
        queue = celery_setup.worker.worker_queue
        sig = long_running_task.si(1).set(queue=queue)
        res = sig.delay()
        assert res.get(timeout=RESULT_TIMEOUT) is True
        self.terminate(celery_setup.worker, termination_method)
        sig = long_running_task.si(1).set(queue=queue)
        res = sig.delay()
        assert res.get(timeout=RESULT_TIMEOUT) is True

    def test_reconnect_to_restarted_worker(
        self,
        celery_setup: CeleryTestSetup,
        termination_method: WorkerTermination,
    ):
        queue = celery_setup.worker.worker_queue
        sig = long_running_task.si(1).set(queue=queue)
        res = sig.delay()
        assert res.get(timeout=10) is True
        for worker in celery_setup.worker_cluster:
            self.terminate(worker, termination_method)
        celery_setup.worker.restart()
        sig = long_running_task.si(1).set(queue=queue)
        res = sig.delay()
        assert res.get(timeout=10) is True

    def test_task_retry_on_worker_crash(
        self,
        celery_setup: CeleryTestSetup,
        termination_method: WorkerTermination,
    ):
        if isinstance(celery_setup.broker, RedisTestBroker):
            pytest.xfail("Potential Bug: works with RabbitMQ, but not Redis")

        sleep_time = 4
        queue = celery_setup.worker.worker_queue
        sig = long_running_task.si(sleep_time, verbose=True).set(queue=queue)
        res = sig.apply_async(retry=True, retry_policy={"max_retries": 1})
        celery_setup.worker.wait_for_log("Sleeping: 2")  # Let task run
        self.terminate(celery_setup.worker, termination_method)
        assert res.get(timeout=10) is True
