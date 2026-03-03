from typing import Any

import pytest
from pytest_celery import CeleryTestWorker, CeleryWorkerContainer, defaults
from pytest_docker_tools import build, container, fxtr

from celery import Celery


class CeleryLatestWorkerContainer(CeleryWorkerContainer):
    """Defines the configurations for a Celery worker container.

    This worker will install the latest version of Celery from PyPI.
    """

    @property
    def client(self) -> Any:
        return self

    @classmethod
    def log_level(cls) -> str:
        return "INFO"

    @classmethod
    def worker_name(cls) -> str:
        return "celery_latest_tests_worker"

    @classmethod
    def worker_queue(cls) -> str:
        return "celery_latest_tests_queue"


# Build the image from the PyPI Dockerfile
celery_latest_worker_image = build(
    path=".",
    dockerfile="t/smoke/workers/docker/pypi",
    tag="t/smoke/worker:latest",
    buildargs=CeleryLatestWorkerContainer.buildargs(),
)


# Define container settings
celery_latest_worker_container = container(
    image="{celery_latest_worker_image.id}",
    environment=fxtr("default_worker_env"),
    network="{default_pytest_celery_network.name}",
    volumes={"{default_worker_volume.name}": defaults.DEFAULT_WORKER_VOLUME},
    wrapper_class=CeleryLatestWorkerContainer,
    timeout=defaults.DEFAULT_WORKER_CONTAINER_TIMEOUT,
    command=CeleryLatestWorkerContainer.command(),
)


@pytest.fixture
def celery_latest_worker(
    celery_latest_worker_container: CeleryLatestWorkerContainer,
    celery_setup_app: Celery,
) -> CeleryTestWorker:
    """Creates a pytest-celery worker node from the worker container."""
    worker = CeleryTestWorker(celery_latest_worker_container, app=celery_setup_app)
    yield worker
    worker.teardown()
