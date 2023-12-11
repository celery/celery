from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

from pytest_celery import CeleryTestWorker

from celery.exceptions import TimeLimitExceeded, WorkerLostError
from t.smoke.tasks import suicide


class TaskTermination:
    class Method(Enum):
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

    def run_suicide_task(
        self,
        worker: CeleryTestWorker,
        method: TaskTermination.Method,
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
            TaskTermination.Method.DELAY_TIMEOUT: TimeLimitExceeded,
            TaskTermination.Method.CPU_OVERLOAD: RecursionError,
            TaskTermination.Method.EXCEPTION: Exception,
            TaskTermination.Method.SYSTEM_EXIT: WorkerLostError,
            TaskTermination.Method.ALLOCATE_MAX_MEMORY: MemoryError,
            TaskTermination.Method.EXHAUST_MEMORY: WorkerLostError,
            TaskTermination.Method.EXHAUST_HDD: OSError,
            TaskTermination.Method.SIGKILL: WorkerLostError,
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
