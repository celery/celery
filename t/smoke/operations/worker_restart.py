from __future__ import annotations

from enum import Enum, auto

from pytest_celery import CeleryTestWorker


class WorkerRestart:
    class Method(Enum):
        POOL_RESTART = auto()
        DOCKER_RESTART_GRACEFULLY = auto()
        DOCKER_RESTART_FORCE = auto()

    def restart_worker(
        self,
        worker: CeleryTestWorker,
        method: WorkerRestart.Method,
        assertion: bool = True,
    ):
        if method == WorkerRestart.Method.POOL_RESTART:
            worker.app.control.pool_restart()
            worker.container.reload()

        if method == WorkerRestart.Method.DOCKER_RESTART_GRACEFULLY:
            worker.restart()

        if method == WorkerRestart.Method.DOCKER_RESTART_FORCE:
            worker.restart(force=True)

        if assertion:
            assert worker.container.status == "running", (
                f"Worker container should be in 'running' state after restart, "
                f"but is in '{worker.container.status}' state instead."
            )
