from __future__ import annotations

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup, CeleryTestWorker, CeleryWorkerCluster
from tenacity import retry, stop_after_attempt, wait_fixed

from celery import Celery, signature
from celery.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded, WorkerLostError
from t.integration.tasks import add, identity
from t.smoke.conftest import SuiteOperations, TaskTermination
from t.smoke.tasks import (replace_with_task, soft_time_limit_lower_than_time_limit,
                           soft_time_limit_must_exceed_time_limit)


class test_task_termination(SuiteOperations):
    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.worker_prefetch_multiplier = 1
        app.conf.worker_concurrency = 1
        return app

    @pytest.mark.parametrize(
        "method,expected_error",
        [
            (TaskTermination.Method.SIGKILL, WorkerLostError),
            (TaskTermination.Method.SYSTEM_EXIT, WorkerLostError),
            (TaskTermination.Method.DELAY_TIMEOUT, TimeLimitExceeded),
            # Exhausting the memory messes up the CI environment
            # (TaskTermination.Method.EXHAUST_MEMORY, WorkerLostError),
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
            self.apply_self_termination_task(celery_setup.worker, method).get()

        # Allowing the worker to respawn the child process before we continue
        @retry(
            stop=stop_after_attempt(42),
            wait=wait_fixed(0.1),
            reraise=True,
        )
        def wait_for_two_celery_processes():
            pinfo_current = celery_setup.worker.get_running_processes_info(
                ["pid", "name"],
                filters={"name": "celery"},
            )
            if len(pinfo_current) != 2:
                assert False, f"Child process did not respawn with method: {method.name}"

        wait_for_two_celery_processes()

        pinfo_after = celery_setup.worker.get_running_processes_info(
            ["pid", "name"],
            filters={"name": "celery"},
        )

        pids_before = {item["pid"] for item in pinfo_before}
        pids_after = {item["pid"] for item in pinfo_after}
        assert len(pids_before | pids_after) == 3

    @pytest.mark.parametrize(
        "method,expected_log,expected_exception_msg",
        [
            (
                TaskTermination.Method.SIGKILL,
                "Worker exited prematurely: signal 9 (SIGKILL)",
                None,
            ),
            (
                TaskTermination.Method.SYSTEM_EXIT,
                "Worker exited prematurely: exitcode 1",
                None,
            ),
            (
                TaskTermination.Method.DELAY_TIMEOUT,
                "Hard time limit (2s) exceeded for t.smoke.tasks.self_termination_delay_timeout",
                "TimeLimitExceeded(2,)",
            ),
            # Exhausting the memory messes up the CI environment
            # (
            #     TaskTermination.Method.EXHAUST_MEMORY,
            #     "Worker exited prematurely: signal 9 (SIGKILL)",
            #     None,
            # ),
        ],
    )
    def test_terminated_task_logs_correct_error(
        self,
        celery_setup: CeleryTestSetup,
        method: TaskTermination.Method,
        expected_log: str,
        expected_exception_msg: str | None,
    ):
        try:
            self.apply_self_termination_task(celery_setup.worker, method).get()
        except Exception as err:
            assert expected_exception_msg or expected_log in str(err)

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


class test_time_limit:
    def test_soft_time_limit_lower_than_time_limit(self, celery_setup: CeleryTestSetup):
        sig = soft_time_limit_lower_than_time_limit.s()
        result = sig.apply_async(queue=celery_setup.worker.worker_queue)
        with pytest.raises(SoftTimeLimitExceeded):
            result.get(timeout=RESULT_TIMEOUT) is None

    def test_soft_time_limit_must_exceed_time_limit(self, celery_setup: CeleryTestSetup):
        sig = soft_time_limit_must_exceed_time_limit.s()
        with pytest.raises(ValueError, match="soft_time_limit must be less than or equal to time_limit"):
            sig.apply_async(queue=celery_setup.worker.worker_queue)
