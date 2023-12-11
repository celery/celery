from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

from pytest_celery import CeleryTestSetup, CeleryTestWorker

from celery.exceptions import TimeLimitExceeded, WorkerLostError
from t.smoke.tasks import suicide


class TaskTermination:
    class Methods(Enum):
        DELAY_TIMEOUT = auto()
        CPU_OVERLOAD = auto()
        EXCEPTION = auto()
        SYSTEM_EXIT = auto()
        ALLOCATE_MAX_MEMORY = auto()
        EXHAUST_MEMORY = auto()
        EXHAUST_HDD = auto()
        CONTROL_SHUTDOWN = auto()
        SIGKILL = auto()

    @dataclass
    class Options:
        worker: CeleryTestWorker
        method: str
        allocate: int
        large_file_name: str
        hostname: str
        try_eager: bool = True
        time_limit: int = 4
        cpu_load_factor: int = 420

    def terminate(
        self,
        worker: CeleryTestWorker,
        method: TaskTermination.Methods,
        **options: dict,
    ):
        # Update kwargs with default values for missing keys
        defaults = {
            "worker": worker,
            "method": method.name,
            "allocate": worker.app.conf.worker_max_memory_per_child * 10**9,
            "large_file_name": worker.name(),
            "hostname": worker.hostname(),
        }
        options = {**defaults, **options}
        options = TaskTermination.Options(**options)

        expected_error = {
            TaskTermination.Methods.DELAY_TIMEOUT: TimeLimitExceeded,
            TaskTermination.Methods.CPU_OVERLOAD: RecursionError,
            TaskTermination.Methods.EXCEPTION: Exception,
            TaskTermination.Methods.SYSTEM_EXIT: WorkerLostError,
            TaskTermination.Methods.ALLOCATE_MAX_MEMORY: MemoryError,
            TaskTermination.Methods.EXHAUST_MEMORY: WorkerLostError,
            TaskTermination.Methods.EXHAUST_HDD: OSError,
            TaskTermination.Methods.SIGKILL: WorkerLostError,
        }.get(method)

        try:
            suicide(**options.__dict__)
        except Exception as e:
            if expected_error is None:
                # No specific error expected, this is an unexpected exception
                assert (
                    False
                ), f"Worker termination by '{method.name}' failed due to an unexpected error: {e}"

            if not isinstance(e, expected_error):
                # Specific error expected but an unexpected type of error occurred
                assert (
                    False
                ), f"Worker termination by '{method.name}' failed due to a different error: {e}"
        finally:
            worker.container.reload()


class WorkerRestart:
    class Methods(Enum):
        POOL_RESTART = auto()
        DOCKER_RESTART_GRACEFULLY = auto()
        DOCKER_RESTART_FORCE = auto()

    def restart(
        self,
        celery_setup: CeleryTestSetup,
        # TODO: Receive worker instead of setup
        method: WorkerRestart.Methods,
    ):
        if method == WorkerRestart.Methods.POOL_RESTART:
            celery_setup.app.control.pool_restart()
        elif method == WorkerRestart.Methods.DOCKER_RESTART_GRACEFULLY:
            celery_setup.worker.restart()
        elif method == WorkerRestart.Methods.DOCKER_RESTART_FORCE:
            celery_setup.worker.restart(force=True)


class WorkerOperations(
    TaskTermination,
    WorkerRestart,
):
    pass
