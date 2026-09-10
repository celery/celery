from __future__ import annotations

from enum import Enum, auto

from pytest_celery import CeleryTestWorker

from celery.canvas import Signature
from celery.result import AsyncResult
from t.smoke.tasks import (self_termination_delay_timeout, self_termination_exhaust_memory, self_termination_sigkill,
                           self_termination_system_exit)


class TaskTermination:
    """Terminates a task in different ways."""
    class Method(Enum):
        SIGKILL = auto()
        SYSTEM_EXIT = auto()
        DELAY_TIMEOUT = auto()
        EXHAUST_MEMORY = auto()

    def apply_self_termination_task(
        self,
        worker: CeleryTestWorker,
        method: TaskTermination.Method,
    ) -> AsyncResult:
        """Apply a task that will terminate itself.

        Args:
            worker (CeleryTestWorker): Take the queue of this worker.
            method (TaskTermination.Method): The method to terminate the task.

        Returns:
            AsyncResult: The result of applying the task.
        """
        try:
            self_termination_sig: Signature = {
                TaskTermination.Method.SIGKILL: self_termination_sigkill.si(),
                TaskTermination.Method.SYSTEM_EXIT: self_termination_system_exit.si(),
                TaskTermination.Method.DELAY_TIMEOUT: self_termination_delay_timeout.si(),
                TaskTermination.Method.EXHAUST_MEMORY: self_termination_exhaust_memory.si(),
            }[method]

            return self_termination_sig.apply_async(queue=worker.worker_queue)
        finally:
            # If there's an unexpected bug and the termination of the task caused the worker
            # to crash, this will refresh the container object with the updated container status
            # which can be asserted/checked during a test (for dev/debug)
            worker.container.reload()
