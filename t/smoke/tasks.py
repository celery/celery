from __future__ import annotations

import math
import os
import sys
from signal import SIGKILL
from sys import getsizeof
from time import sleep

import celery.utils
from celery import Task, shared_task, signature
from celery.app.control import Control
from celery.canvas import Signature
from t.integration.tasks import *  # noqa
from t.integration.tasks import replaced_with_me


@shared_task
def noop(*args, **kwargs) -> None:
    return celery.utils.noop(*args, **kwargs)


@shared_task
def long_running_task(seconds: float = 1, verbose: bool = False) -> bool:
    from celery import current_task
    from celery.utils.log import get_task_logger

    logger = get_task_logger(current_task.name)

    logger.info("Starting long running task")

    for i in range(0, int(seconds)):
        sleep(1)
        if verbose:
            logger.info(f"Sleeping: {i}")

    logger.info("Finished long running task")

    return True


@shared_task(bind=True)
def replace_with_task(self: Task, replace_with: Signature = None):
    if replace_with is None:
        replace_with = replaced_with_me.s()
    return self.replace(signature(replace_with))


@shared_task
def suicide(
    method: str,
    try_eager: bool = True,
    **options,
):
    termination_method = {
        "DELAY_TIMEOUT": suicide_delay_timeout.si(
            time_limit=options["time_limit"],
        ),
        "CPU_OVERLOAD": suicide_cpu_overload.si(
            cpu_load_factor=options["cpu_load_factor"]
        ),
        "EXCEPTION": suicide_exception.si(),
        "SYSTEM_EXIT": suicide_system_exit.si(),
        "ALLOCATE_MAX_MEMORY": suicide_allocate_max_memory.si(
            allocate=options["allocate"]
        ),
        "EXHAUST_MEMORY": suicide_exhaust_memory.si(),
        "EXHAUST_HDD": suicide_exhaust_hdd.si(
            large_file_name=options["large_file_name"]
        ),
        "CONTROL_SHUTDOWN": suicide_control_shutdown.si(
            hostname=options["hostname"],
        ),
        "SIGKILL": suicide_sigkill.si(),
    }

    sig = termination_method.get(method)
    if sig:
        if try_eager and method in {
            "CONTROL_SHUTDOWN",
        }:
            return sig.apply().get()

        worker = options["worker"]
        return sig.apply_async(queue=worker.worker_queue).get()
    else:
        raise ValueError(f"Unsupported termination method: {method}")


@shared_task(time_limit=2)
def suicide_delay_timeout(time_limit: int = 4):
    """Delays the execution to simulate a task timeout."""
    sleep(time_limit)


@shared_task
def suicide_cpu_overload(cpu_load_factor: int = 420):
    """Performs CPU-intensive operations to simulate a CPU overload."""

    def cpu_intensive_calculation(n):
        return cpu_intensive_calculation(math.sin(n))

    cpu_intensive_calculation(cpu_load_factor)


@shared_task
def suicide_exception():
    """Raises an exception to simulate an unexpected error during task execution."""
    raise Exception("Simulated task failure due to an exception.")


@shared_task
def suicide_system_exit():
    """Triggers a system exit to simulate a critical stop of the Celery worker."""
    sys.exit("Simulated Celery worker stop via system exit.")


@shared_task
def suicide_allocate_max_memory(allocate: int):
    """Allocates the maximum amount of memory permitted, potentially leading to memory errors."""
    _ = [0] * (allocate // getsizeof(int()))


@shared_task
def suicide_exhaust_memory():
    """Continuously allocates memory to simulate memory exhaustion."""
    mem = []
    while True:
        mem.append(" " * 10**6)


@shared_task
def suicide_exhaust_hdd(large_file_name: str = "large_file"):
    """Consumes disk space in /tmp to simulate a scenario where the disk is getting full."""
    # file_path = f"/tmp/{large_file_name}.tmp"
    # try:
    #     with open(file_path, "wb") as f:
    #         chunk = b"\0" * 42 * 1024**2  # 42 MB
    #         while True:
    #             f.write(chunk)
    # finally:
    #     if os.path.exists(file_path):
    #         os.remove(file_path)

    # This code breaks GitHub CI so we simulate the same error as best effort
    #########################################################################
    # [error]Failed to create step summary using 'GITHUB_STEP_SUMMARY': No space left on device
    # [error]No space left on device
    raise OSError("No space left on device")


@shared_task
def suicide_control_shutdown(hostname: str):
    """Initiates a controlled shutdown via the Control API, simulating a graceful termination."""
    from celery.app.base import get_current_app

    app = get_current_app()
    control: Control = app.control
    control.shutdown(destination=[hostname])


@shared_task
def suicide_sigkill():
    """Terminates the worker, simulating a forceful shutdown similar to a SIGKILL signal."""
    os.kill(os.getpid(), SIGKILL)
