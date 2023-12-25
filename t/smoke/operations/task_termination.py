from __future__ import annotations

from enum import Enum, auto
from time import sleep

from pytest_celery import CeleryTestWorker

from t.smoke.tasks import suicide_delay_timeout, suicide_exhaust_memory, suicide_sigkill, suicide_system_exit


class TaskTermination:
    class Method(Enum):
        SIGKILL = auto()
        SYSTEM_EXIT = auto()
        DELAY_TIMEOUT = auto()
        EXHAUST_MEMORY = auto()

    def apply_suicide_task(
        self,
        worker: CeleryTestWorker,
        method: TaskTermination.Method,
    ):
        try:
            suicide_sig = {
                TaskTermination.Method.SIGKILL: suicide_sigkill.si(),
                TaskTermination.Method.SYSTEM_EXIT: suicide_system_exit.si(),
                TaskTermination.Method.DELAY_TIMEOUT: suicide_delay_timeout.si(),
                TaskTermination.Method.EXHAUST_MEMORY: suicide_exhaust_memory.si(),
            }[method]

            return suicide_sig.apply_async(queue=worker.worker_queue).get()
        finally:
            # If there's an unexpected bug and the termination of the task caused the worker
            # to crash, this will refresh the container object with the updated container status
            # which can be asserted/checked during a test (for dev/debug)
            worker.container.reload()

            # Allowing the worker to respawn the child process before we continue
            sleep(0.5)
