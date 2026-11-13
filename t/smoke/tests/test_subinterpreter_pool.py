"""Smoke tests for the subinterpreter pool (``--pool=subinterpreter``).

The pool needs Python 3.14+, so the worker runs the dev image built on
Python 3.14 whatever the version running the tests.  It only loads
t.smoke.subinterpreter_tasks, see there why.
"""

from __future__ import annotations

import os
from time import sleep
from types import ModuleType

import pytest
from pytest_celery import CeleryTestSetup, CeleryTestWorker, CeleryWorkerContainer, defaults
from pytest_docker_tools import build, container, fxtr

from celery import states
from celery.canvas import chain, group
from t.smoke import subinterpreter_tasks
from t.smoke.conftest import SuiteOperations, WorkerKill
from t.smoke.subinterpreter_tasks import add, current_interpreter_id, fail, long_running_task, stoppable_task
from t.smoke.workers.dev import SmokeWorkerContainer

RESULT_TIMEOUT = 30
CONCURRENCY = 2

celery_subinterpreter_worker_image = build(
    path=".",
    dockerfile="t/smoke/workers/docker/dev",
    tag="t/smoke/worker:subinterpreter",
    buildargs={**SmokeWorkerContainer.buildargs(), "PYTHON_VERSION": "3.14"},
)

# Same container as t/smoke/workers/dev.py, on the Python 3.14 image.
# Module-level definition shadows the default worker container for tests in
# this file only.
default_worker_container = container(
    image="{celery_subinterpreter_worker_image.id}",
    ports=fxtr("default_worker_ports"),
    environment=fxtr("default_worker_env"),
    network="{default_pytest_celery_network.name}",
    volumes={
        "{default_worker_volume.name}": defaults.DEFAULT_WORKER_VOLUME,
        os.path.abspath(os.getcwd()): {
            "bind": "/celery",
            "mode": "rw",
        },
    },
    wrapper_class=SmokeWorkerContainer,
    timeout=defaults.DEFAULT_WORKER_CONTAINER_TIMEOUT,
    command=fxtr("default_worker_command"),
)


@pytest.fixture
def default_worker_tasks() -> set[ModuleType]:
    return {subinterpreter_tasks}


@pytest.fixture
def default_worker_command(default_worker_container_cls: type[CeleryWorkerContainer]) -> list[str]:
    return default_worker_container_cls.command(
        "--pool=subinterpreter", f"--concurrency={CONCURRENCY}",
    )


def assert_container_exited(worker: CeleryTestWorker, attempts: int = RESULT_TIMEOUT):
    """It might take a few moments for the container to exit after the worker is killed."""
    while attempts:
        worker.container.reload()
        if worker.container.status == "exited":
            break
        attempts -= 1
        sleep(1)
    worker.container.reload()
    assert worker.container.status == "exited"


def get_pool_stats(setup: CeleryTestSetup, worker: CeleryTestWorker, attempts: int = 10) -> dict:
    """Fetch the worker's pool stats over the control channel, with retries
    while the worker finishes booting."""
    last = None
    for _ in range(attempts):
        last = setup.app.control.inspect(
            destination=[worker.hostname()], timeout=3
        ).stats()
        if last and worker.hostname() in last:
            return last[worker.hostname()]["pool"]
        sleep(1)
    raise AssertionError(f"No stats reply from {worker.hostname()}: {last!r}")


class test_subinterpreter_pool:
    def test_pool_stats(self, celery_setup: CeleryTestSetup):
        pool = get_pool_stats(celery_setup, celery_setup.worker)
        assert pool["implementation"] == "celery.concurrency.subinterpreter:TaskPool"
        assert pool["max-concurrency"] == CONCURRENCY
        assert pool["interpreters"] == CONCURRENCY

    def test_tasks_run_in_subinterpreters(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        sig = group(current_interpreter_id.si() for _ in range(10)).set(queue=queue)
        ids = set(sig.delay().get(timeout=RESULT_TIMEOUT))
        # 0 is the main interpreter.
        assert ids and 0 not in ids
        assert len(ids) <= CONCURRENCY

    def test_canvas(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        sig = chain(
            add.si(1, 2).set(queue=queue),
            add.s(3).set(queue=queue),
            group(add.s(i).set(queue=queue) for i in range(3)),
        )
        assert sig.delay().get(timeout=RESULT_TIMEOUT) == [6, 7, 8]

    def test_task_failure(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        res = fail.si().set(queue=queue).delay()
        with pytest.raises(ValueError, match="Task expected to fail"):
            res.get(timeout=RESULT_TIMEOUT)


class test_subinterpreter_pool_shutdown(SuiteOperations):
    def test_warm_shutdown(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        worker = celery_setup.worker
        sig = long_running_task.si(5).set(queue=queue)
        res = sig.delay()
        worker.assert_log_exists("Starting long running task")
        self.kill_worker(worker, WorkerKill.Method.SIGTERM)
        worker.assert_log_exists("worker: Warm shutdown (MainProcess)")
        assert_container_exited(worker)
        assert res.get(RESULT_TIMEOUT)

    def test_cold_shutdown_runs_task_to_completion(self, celery_setup: CeleryTestSetup):
        # A running subinterpreter cannot be interrupted.
        queue = celery_setup.worker.worker_queue
        worker = celery_setup.worker
        sig = long_running_task.si(5).set(queue=queue)
        res = sig.delay()
        worker.assert_log_exists("Starting long running task")
        self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
        worker.assert_log_exists("worker: Cold shutdown (MainProcess)")
        worker.assert_log_exists("cannot terminate a running task")
        assert_container_exited(worker)
        assert res.get(RESULT_TIMEOUT)
        worker.assert_log_does_not_exist("Traceback", timeout=1)

    def test_cold_shutdown_stops_stoppable_task(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        worker = celery_setup.worker
        sig = stoppable_task.si(120).set(queue=queue)
        sig.delay()
        worker.assert_log_exists("Starting stoppable task")
        self.kill_worker(worker, WorkerKill.Method.SIGQUIT)
        worker.assert_log_exists("Stoppable task asked to stop")
        assert_container_exited(worker)


class test_subinterpreter_pool_stop:
    def test_revoke_terminate_stops_stoppable_task(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        worker = celery_setup.worker
        res = stoppable_task.si(120).set(queue=queue).delay()
        worker.assert_log_exists("Starting stoppable task")
        res.revoke(terminate=True)
        worker.assert_log_exists("Stoppable task asked to stop")
        worker.assert_log_exists(f"stoppable_task[{res.id}] ignored")
        assert res.state == states.REVOKED

    def test_soft_time_limit_stops_stoppable_task(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        worker = celery_setup.worker
        res = stoppable_task.si(120).set(queue=queue, soft_time_limit=2).delay()
        worker.assert_log_exists("Soft time limit (2s) exceeded")
        worker.assert_log_exists(f"stoppable_task[{res.id}] ignored")


class test_subinterpreter_pool_max_tasks_per_child:
    @pytest.fixture
    def default_worker_command(self, default_worker_container_cls: type[CeleryWorkerContainer]) -> list[str]:
        return default_worker_container_cls.command(
            "--pool=subinterpreter", "--concurrency=1", "--max-tasks-per-child=2",
        )

    def test_interpreter_replaced(self, celery_setup: CeleryTestSetup):
        queue = celery_setup.worker.worker_queue
        ids = [
            current_interpreter_id.si().set(queue=queue).delay().get(timeout=RESULT_TIMEOUT)
            for _ in range(5)
        ]
        assert ids[0] == ids[1] != ids[2] == ids[3] != ids[4]
        pool = get_pool_stats(celery_setup, celery_setup.worker)
        assert pool["max-tasks-per-child"] == 2
        assert pool["interpreters"] == 1
