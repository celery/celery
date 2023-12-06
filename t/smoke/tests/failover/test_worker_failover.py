from __future__ import annotations

import pytest
from pytest_celery import CeleryTestSetup, CeleryTestWorker, CeleryWorkerCluster, RedisTestBroker

from celery import Celery
from celery.app.control import Control
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
        "control.shutdown",
        "memory_limit",
    ],
)
class test_worker_failover:
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.task_acks_late = True
        app.conf.worker_max_memory_per_child = 10 * 1024  # Limit to 10MB
        if app.conf.broker_url.startswith("redis"):
            app.conf.broker_transport_options = {"visibility_timeout": 1}
        yield app

    def terminate(self, worker: CeleryTestWorker, method: str):
        if method == "SIGKILL":
            # Reduces actual workers count by 1
            worker.kill()
        elif method == "control.shutdown":
            # Completes the task and then shuts down the worker
            control: Control = worker.app.control
            control.shutdown(destination=[worker.hostname()])
        elif method == "memory_limit":
            # Child process is killed and a new one is spawned, but the worker is not terminated
            allocate = worker.app.conf.worker_max_memory_per_child * 1_000_000_000
            sig = long_running_task.si(allocate=allocate).set(queue=worker.worker_queue)
            sig.delay()

    def test_killing_first_worker(
        self,
        celery_setup: CeleryTestSetup,
        termination_method: str,
    ):
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
        termination_method: str,
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
        termination_method: str,
    ):
        if isinstance(celery_setup.broker, RedisTestBroker):
            pytest.xfail("Potential Bug: works with RabbitMQ, but not Redis")

        sleep_time = 4
        queue = celery_setup.worker.worker_queue
        sig = long_running_task.si(sleep_time, verbose=True).set(queue=queue)
        res = sig.apply_async(retry=True, retry_policy={"max_retries": 1})
        celery_setup.worker.wait_for_log("Sleeping: 2")  # Wait for the task to run a bit
        self.terminate(celery_setup.worker, termination_method)
        assert res.get(timeout=10) is True
