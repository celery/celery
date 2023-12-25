import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup, CeleryTestWorker, CeleryWorkerCluster

from celery import Celery, signature
from celery.exceptions import TimeLimitExceeded, WorkerLostError
from t.integration.tasks import add, identity
from t.smoke.conftest import SuiteOperations, TaskTermination
from t.smoke.tasks import replace_with_task


class test_task_termination(SuiteOperations):
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.worker_prefetch_multiplier = 1
        app.conf.worker_concurrency = 1
        yield app

    @pytest.mark.parametrize(
        "method,expected_error",
        [
            (TaskTermination.Method.SIGKILL, WorkerLostError),
            (TaskTermination.Method.SYSTEM_EXIT, WorkerLostError),
            (TaskTermination.Method.DELAY_TIMEOUT, TimeLimitExceeded),
            (TaskTermination.Method.EXHAUST_MEMORY, WorkerLostError),
        ],
    )
    def test_child_process_respawn(
        self,
        celery_setup: CeleryTestSetup,
        method: TaskTermination.Method,
        expected_error: Exception,
    ):
        pinfo_before = celery_setup.worker.get_running_processes_info(
            ["pid", "name"],
            filters={"name": "celery"},
        )

        with pytest.raises(expected_error):
            self.apply_suicide_task(celery_setup.worker, method)

        pinfo_after = celery_setup.worker.get_running_processes_info(
            ["pid", "name"],
            filters={"name": "celery"},
        )

        pids_before = set(item["pid"] for item in pinfo_before)
        pids_after = set(item["pid"] for item in pinfo_after)
        assert len(pids_before | pids_after) == 3

    @pytest.mark.parametrize(
        "method,expected_log",
        [
            (
                TaskTermination.Method.SIGKILL,
                "Worker exited prematurely: signal 9 (SIGKILL)",
            ),
            (
                TaskTermination.Method.SYSTEM_EXIT,
                "Worker exited prematurely: exitcode 1",
            ),
            (
                TaskTermination.Method.DELAY_TIMEOUT,
                "Hard time limit (2s) exceeded for t.smoke.tasks.suicide_delay_timeout",
            ),
            (
                TaskTermination.Method.EXHAUST_MEMORY,
                "Worker exited prematurely: signal 9 (SIGKILL)",
            ),
        ],
    )
    def test_terminated_task_logs(
        self,
        celery_setup: CeleryTestSetup,
        method: TaskTermination.Method,
        expected_log: str,
    ):
        with pytest.raises(Exception):
            self.apply_suicide_task(celery_setup.worker, method)

        celery_setup.worker.assert_log_exists(expected_log)


class test_replace:
    @pytest.fixture
    def celery_worker_cluster(
        self,
        celery_worker: CeleryTestWorker,
        celery_other_dev_worker: CeleryTestWorker,
    ) -> CeleryWorkerCluster:
        cluster = CeleryWorkerCluster(celery_worker, celery_other_dev_worker)
        yield cluster
        cluster.teardown()

    def test_sanity(self, celery_setup: CeleryTestSetup):
        queues = [w.worker_queue for w in celery_setup.worker_cluster]
        assert len(queues) == 2
        assert queues[0] != queues[1]
        replace_with = signature(identity, args=(40,), queue=queues[1])
        sig1 = replace_with_task.s(replace_with)
        sig2 = add.s(2).set(queue=queues[1])
        c = sig1 | sig2
        r = c.apply_async(queue=queues[0])
        assert r.get(timeout=RESULT_TIMEOUT) == 42
