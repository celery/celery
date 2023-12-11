from __future__ import annotations

from enum import Enum, auto

from pytest_celery import CeleryTestSetup, CeleryTestWorker

from celery.exceptions import TimeLimitExceeded, WorkerLostError
from t.smoke.tasks import suicide


class WorkerTermination:
    class Methods(Enum):
        DELAY_TIMEOUT = auto()
        CPU_OVERLOAD = auto()
        EXCEPTION = auto()
        SYSTEM_EXIT = auto()
        ALLOCATE_MAX_MEMORY = auto()
        EXHAUST_MEMORY = auto()
        EXHAUST_HDD = auto()
        CONTROL_SHUTDOWN = auto()
        FORCEFUL_TERMINATION = auto()

    def terminate(
        self,
        worker: CeleryTestWorker,
        method: WorkerTermination.Methods,
        try_eager: bool = True,
        **kwargs: dict,
    ):
        options = dict(
            worker=worker,
            method=method.name,
            try_eager=try_eager,
            time_limit=kwargs.get("time_limit", 4),
            cpu_load_factor=kwargs.get("cpu_load_factor", 420),
            allocate=kwargs.get(
                "allocate",
                worker.app.conf.worker_max_memory_per_child * 10**9,
            ),
            large_file_name=kwargs.get("large_file_name", worker.name()),
            hostname=kwargs.get("hostname", worker.hostname()),
        )

        expected_error = {
            WorkerTermination.Methods.DELAY_TIMEOUT: TimeLimitExceeded,
            WorkerTermination.Methods.CPU_OVERLOAD: RecursionError,
            WorkerTermination.Methods.EXCEPTION: Exception,
            WorkerTermination.Methods.SYSTEM_EXIT: WorkerLostError,
            WorkerTermination.Methods.ALLOCATE_MAX_MEMORY: MemoryError,
            WorkerTermination.Methods.EXHAUST_MEMORY: WorkerLostError,
            WorkerTermination.Methods.EXHAUST_HDD: (
                # When HD is getting full
                OSError,
                # When HD is full already before we start allocating
                WorkerLostError,
                # When the allocation is bigger than the available memory
                MemoryError,  # Dependent on local docker memory settings
            ),
            WorkerTermination.Methods.FORCEFUL_TERMINATION: WorkerLostError,
        }.get(method)

        try:
            suicide(**options)
        except BaseException as e:
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


class WorkerRestart:
    class Methods(Enum):
        POOL_RESTART = auto()
        DOCKER_RESTART_GRACEFULLY = auto()
        DOCKER_RESTART_FORCE = auto()

    def restart(
        self,
        celery_setup: CeleryTestSetup,
        method: WorkerRestart,
    ):
        if method == WorkerRestart.Methods.POOL_RESTART:
            celery_setup.app.control.pool_restart()
        elif method == WorkerRestart.Methods.DOCKER_RESTART_GRACEFULLY:
            celery_setup.worker.restart()
        elif method == WorkerRestart.Methods.DOCKER_RESTART_FORCE:
            celery_setup.worker.restart(force=True)


class WorkerOperations(
    WorkerTermination,
    WorkerRestart,
):
    pass
