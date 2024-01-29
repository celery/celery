from __future__ import annotations

import os

import pytest
from pytest_celery import CeleryTestWorker, defaults
from pytest_docker_tools import build, container, fxtr

from celery import Celery
from t.smoke.workers.dev import SmokeWorkerContainer


class AltSmokeWorkerContainer(SmokeWorkerContainer):
    @classmethod
    def worker_name(cls) -> str:
        return "alt_smoke_tests_worker"


celery_alt_dev_worker_image = build(
    path=".",
    dockerfile="t/smoke/workers/docker/dev",
    tag="t/smoke/worker:alt",
    buildargs=AltSmokeWorkerContainer.buildargs(),
)


alt_dev_worker_container = container(
    image="{celery_alt_dev_worker_image.id}",
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
    wrapper_class=AltSmokeWorkerContainer,
    timeout=defaults.DEFAULT_WORKER_CONTAINER_TIMEOUT,
)


@pytest.fixture
def celery_alt_dev_worker(
    alt_dev_worker_container: AltSmokeWorkerContainer,
    celery_setup_app: Celery,
) -> CeleryTestWorker:
    worker = CeleryTestWorker(alt_dev_worker_container, app=celery_setup_app)
    yield worker
    worker.teardown()
