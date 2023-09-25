from typing import Any

from pytest_celery.containers.worker import CeleryWorkerContainer

import celery


class SmokeWorkerContainer(CeleryWorkerContainer):
    @property
    def client(self) -> Any:
        return self

    @classmethod
    def version(cls) -> str:
        return celery.__version__

    @classmethod
    def log_level(cls) -> str:
        return "INFO"

    @classmethod
    def worker_name(cls) -> str:
        return "smoke_tests_worker"

    @classmethod
    def worker_queue(cls) -> str:
        return "smoke_tests_queue"
