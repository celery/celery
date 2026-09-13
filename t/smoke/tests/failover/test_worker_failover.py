from __future__ import annotations

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup, CeleryTestWorker, CeleryWorkerCluster

from celery import Celery
from t.smoke.conftest import SuiteOperations, WorkerKill
from t.smoke.tasks import long_running_task


@pytest.fixture
def celery_worker_cluster(
    celery_worker: CeleryTestWorker,
    celery_alt_dev_worker: CeleryTestWorker,
) -> CeleryWorkerCluster:
    cluster = CeleryWorkerCluster(celery_worker, celery_alt_dev_worker)
    yield cluster
    cluster.teardown()


@pytest.mark.parametrize("method", [WorkerKill.Method.DOCKER_KILL])
class test_worker_failover(SuiteOperations):
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.task_acks_late = True
        return app

    def test_killing_first_worker(
        self,
        celery_setup: CeleryTestSetup,
        method: WorkerKill.Method,
    ):
        assert len(celery_setup.worker_cluster) > 1

        queue = celery_setup.worker.worker_queue
        self.kill_worker(celery_setup.worker, method)
        sig = long_running_task.si(1).set(queue=queue)
        res = sig.delay()
        assert res.get(timeout=RESULT_TIMEOUT) is True

    def test_reconnect_to_restarted_worker(
        self,
        celery_setup: CeleryTestSetup,
        method: WorkerKill.Method,
    ):
        assert len(celery_setup.worker_cluster) > 1

        queue = celery_setup.worker.worker_queue
        for worker in celery_setup.worker_cluster:
            self.kill_worker(worker, method)
        celery_setup.worker.restart()
        sig = long_running_task.si(1).set(queue=queue)
        res = sig.delay()
        assert res.get(timeout=RESULT_TIMEOUT) is True
