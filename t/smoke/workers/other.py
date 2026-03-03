from __future__ import annotations

import os

import pytest
from pytest_celery import CeleryTestWorker, defaults
from pytest_docker_tools import build, container, fxtr

from celery import Celery
from t.smoke.workers.dev import SmokeWorkerContainer


class OtherSmokeWorkerContainer(SmokeWorkerContainer):
    """Alternative worker with different name and queue, but same configurations for the rest."""

    @classmethod
    def worker_name(cls) -> str:
        return "other_smoke_tests_worker"

    @classmethod
    def worker_queue(cls) -> str:
        return "other_smoke_tests_queue"


# Build the image like the dev worker
celery_other_dev_worker_image = build(
    path=".",
    dockerfile="t/smoke/workers/docker/dev",
    tag="t/smoke/worker:other",
    buildargs=OtherSmokeWorkerContainer.buildargs(),
)


# Define container settings like the dev worker
other_dev_worker_container = container(
    image="{celery_other_dev_worker_image.id}",
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
    wrapper_class=OtherSmokeWorkerContainer,
    timeout=defaults.DEFAULT_WORKER_CONTAINER_TIMEOUT,
    command=OtherSmokeWorkerContainer.command(),
)


@pytest.fixture
def celery_other_dev_worker(
    other_dev_worker_container: OtherSmokeWorkerContainer,
    celery_setup_app: Celery,
) -> CeleryTestWorker:
    """Creates a pytest-celery worker node from the worker container."""
    worker = CeleryTestWorker(other_dev_worker_container, app=celery_setup_app)
    yield worker
    worker.teardown()
