import pytest
from pytest_celery import defaults
from pytest_celery.api.components.worker.node import CeleryTestWorker
from pytest_celery.containers.worker import CeleryWorkerContainer
from pytest_docker_tools import build, container, fxtr

from celery import Celery
from t.smoke.workers.celery4.api import Celery4WorkerContainer

celery4_worker_image = build(
    path="t/smoke/workers/celery4",
    tag="t/smoke/worker:celery4",
    buildargs=Celery4WorkerContainer.buildargs(),
)


celery4_worker_container = container(
    image="{celery4_worker_image.id}",
    environment=fxtr("default_worker_env"),
    network="{DEFAULT_NETWORK.name}",
    volumes={"{default_worker_volume.name}": defaults.DEFAULT_WORKER_VOLUME},
    wrapper_class=Celery4WorkerContainer,
    timeout=defaults.DEFAULT_WORKER_CONTAINER_TIMEOUT,
)


@pytest.fixture
def celery4_worker(celery4_worker_container: CeleryWorkerContainer, celery_setup_app: Celery) -> CeleryTestWorker:
    yield CeleryTestWorker(celery4_worker_container, app=celery_setup_app)
