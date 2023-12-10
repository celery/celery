from __future__ import annotations

from enum import Enum, auto

from billiard.exceptions import WorkerLostError
from pytest_celery import CeleryTestSetup, CeleryTestWorker

from celery.app.control import Control
from t.smoke.tasks import long_running_task


class WorkerOperations:
    class TerminationMethod(Enum):
        SIGKILL = auto()
        CONTROL_SHUTDOWN = auto()
        MAX_MEMORY_ALLOCATED = auto()
        MEMORY_LIMIT_EXCEEDED = auto()

    class RestartMethod(Enum):
        POOL_RESTART = auto()
        DOCKER_RESTART_GRACEFULLY = auto()
        DOCKER_RESTART_FORCE = auto()

    def terminate(self, worker: CeleryTestWorker, method: TerminationMethod):
        if method == WorkerOperations.TerminationMethod.SIGKILL:
            worker.kill()
            return

        if method == WorkerOperations.TerminationMethod.CONTROL_SHUTDOWN:
            control: Control = worker.app.control
            control.shutdown(destination=[worker.hostname()])
            return

        if method == WorkerOperations.TerminationMethod.MAX_MEMORY_ALLOCATED:
            allocate = worker.app.conf.worker_max_memory_per_child * 10**6
            try:
                (
                    long_running_task.si(allocate=allocate)
                    .apply_async(queue=worker.worker_queue)
                    .get()
                )
            except MemoryError:
                return

        if method == WorkerOperations.TerminationMethod.MEMORY_LIMIT_EXCEEDED:
            try:
                (
                    long_running_task.si(exhaust_memory=True)
                    .apply_async(queue=worker.worker_queue)
                    .get()
                )
            except WorkerLostError:
                return

        assert False

    def restart(self, celery_setup: CeleryTestSetup, method: RestartMethod):
        if method == WorkerOperations.RestartMethod.POOL_RESTART:
            celery_setup.app.control.pool_restart()
        elif method == WorkerOperations.RestartMethod.DOCKER_RESTART_GRACEFULLY:
            celery_setup.worker.restart()
        elif method == WorkerOperations.RestartMethod.DOCKER_RESTART_FORCE:
            celery_setup.worker.restart(force=True)
