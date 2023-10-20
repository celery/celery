from typing import Any

import pytest
from pytest_celery import CeleryTestWorker, CeleryWorkerContainer, defaults
from pytest_docker_tools import build, container, fxtr

from celery import Celery


class CeleryLegacyWorkerContainer(CeleryWorkerContainer):
    @property
    def client(self) -> Any:
        return self

    @classmethod
    def version(cls) -> str:
        return "4.4.7"  # Last version of 4.x

    @classmethod
    def log_level(cls) -> str:
        return "DEBUG"

    @classmethod
    def worker_name(cls) -> str:
        return "celery4_tests_worker"

    @classmethod
    def worker_queue(cls) -> str:
        return "celery4_tests_queue"


celery_legacy_worker_image = build(
    path=".",
    dockerfile="t/smoke/workers/docker/pypi",
    tag="t/smoke/worker:legacy",
    buildargs=CeleryLegacyWorkerContainer.buildargs(),
)


celery_legacy_worker_container = container(
    image="{celery_legacy_worker_image.id}",
    environment=fxtr("default_worker_env"),
    network="{default_pytest_celery_network.name}",
    volumes={"{default_worker_volume.name}": defaults.DEFAULT_WORKER_VOLUME},
    wrapper_class=CeleryLegacyWorkerContainer,
    timeout=defaults.DEFAULT_WORKER_CONTAINER_TIMEOUT,
)


@pytest.fixture
def celery_legacy_worker(
    celery_legacy_worker_container: CeleryLegacyWorkerContainer,
    celery_setup_app: Celery,
) -> CeleryTestWorker:
    yield CeleryTestWorker(celery_legacy_worker_container, app=celery_setup_app)
