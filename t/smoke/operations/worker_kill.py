from __future__ import annotations

from enum import Enum, auto

from pytest_celery import CeleryTestWorker

from celery.app.control import Control


class WorkerKill:
    """Kills a worker in different ways."""

    class Method(Enum):
        DOCKER_KILL = auto()
        CONTROL_SHUTDOWN = auto()
        SIGTERM = auto()
        SIGQUIT = auto()

    def kill_worker(
        self,
        worker: CeleryTestWorker,
        method: WorkerKill.Method,
    ) -> None:
        """Kill a Celery worker.

        Args:
            worker (CeleryTestWorker): Worker to kill.
            method (WorkerKill.Method): The method to kill the worker.
        """
        if method == WorkerKill.Method.DOCKER_KILL:
            worker.kill()

            assert worker.container.status == "exited", (
                f"Worker container should be in 'exited' state after kill, "
                f"but is in '{worker.container.status}' state instead."
            )

        if method == WorkerKill.Method.CONTROL_SHUTDOWN:
            control: Control = worker.app.control
            control.shutdown(destination=[worker.hostname()])
            worker.container.reload()

        if method == WorkerKill.Method.SIGTERM:
            worker.kill(signal="SIGTERM")

        if method == WorkerKill.Method.SIGQUIT:
            worker.kill(signal="SIGQUIT")
