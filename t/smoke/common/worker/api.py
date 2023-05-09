from typing import Any

from pytest_celery.containers.worker import CeleryWorkerContainer


class SmokeWorkerContainer(CeleryWorkerContainer):
    @property
    def client(self) -> Any:
        return self

    @classmethod
    def version(cls) -> str:
        return "5.2.7"

    @classmethod
    def log_level(cls) -> str:
        return "INFO"

    @classmethod
    def worker_name(cls) -> str:
        return "celery_smoke_worker"

    @classmethod
    def worker_queue(cls) -> str:
        return "celery_smoke_tests"
