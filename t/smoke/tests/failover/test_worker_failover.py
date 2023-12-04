import pytest
from pytest_celery import CeleryTestSetup, CeleryTestWorker, CeleryWorkerCluster

from celery import Celery
from t.smoke.tasks import long_running_task


@pytest.fixture
def celery_worker_cluster(
    celery_worker: CeleryTestWorker,
    celery_alt_dev_worker: CeleryTestWorker,
) -> CeleryWorkerCluster:
    cluster = CeleryWorkerCluster(celery_worker, celery_alt_dev_worker)
    yield cluster
    cluster.teardown()


@pytest.mark.parametrize(
    "termination_method",
    [
        "SIGKILL",
    ],
)
class test_worker_failover:
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.task_acks_late = True
        yield app

    def terminate(self, worker: CeleryTestWorker, method: str):
        if method == "SIGKILL":
            worker.kill()

    def test_killing_first_worker(
        self,
        celery_setup: CeleryTestSetup,
        termination_method: str,
    ):
        if len(celery_setup.worker_cluster) < 2:
            pytest.skip("Not enough workers in cluster")

        queue = celery_setup.worker.worker_queue
        sig = long_running_task.si(1).set(queue=queue)
        res = sig.delay()
        assert res.get(timeout=2) is True
        self.terminate(celery_setup.worker, termination_method)
        sig = long_running_task.si(1).set(queue=queue)
        res = sig.delay()
        assert res.get(timeout=2) is True

    def test_reconnect_to_restarted_worker(
        self,
        celery_setup: CeleryTestSetup,
        celery_alt_dev_worker: CeleryTestWorker,
        termination_method: str,
    ):
        if len(celery_setup.worker_cluster) < 2:
            pytest.skip("Not enough workers in cluster")

        queue = celery_setup.worker.worker_queue
        sig = long_running_task.si(1).set(queue=queue)
        res = sig.delay()
        assert res.get(timeout=2) is True
        self.terminate(celery_setup.worker, termination_method)
        self.terminate(celery_alt_dev_worker, termination_method)
        celery_setup.worker.restart()
        sig = long_running_task.si(1).set(queue=queue)
        res = sig.delay()
        assert res.get(timeout=2) is True
