import os
from typing import Any, Type

import pytest
from pytest_celery import CeleryWorkerContainer, defaults
from pytest_docker_tools import build, container, fxtr

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


celery_dev_worker_image = build(
    path=".",
    dockerfile="t/smoke/workers/docker/dev",
    tag="t/smoke/worker:dev",
    buildargs=SmokeWorkerContainer.buildargs(),
)


default_worker_container = container(
    image="{celery_dev_worker_image.id}",
    environment=fxtr("default_worker_env"),
    network="{default_pytest_celery_network.name}",
    volumes={
        # Volume: Worker /app
        "{default_worker_volume.name}": defaults.DEFAULT_WORKER_VOLUME,
        # Mount: Celery source
        os.path.abspath(os.getcwd()): {
            "bind": "/celery",
            "mode": "rw",
        },
    },
    wrapper_class=SmokeWorkerContainer,
    timeout=defaults.DEFAULT_WORKER_CONTAINER_TIMEOUT,
)


@pytest.fixture
def default_worker_container_cls() -> Type[CeleryWorkerContainer]:
    return SmokeWorkerContainer


@pytest.fixture(scope="session")
def default_worker_container_session_cls() -> Type[CeleryWorkerContainer]:
    return SmokeWorkerContainer
