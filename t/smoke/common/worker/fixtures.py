from typing import Type

import pytest
from pytest_celery import defaults
from pytest_celery.containers.worker import CeleryWorkerContainer
from pytest_docker_tools import build, container, fxtr

from t.smoke.common.worker.api import SmokeWorkerContainer

smoke_worker_image = build(
    path="t/smoke/common/worker",
    tag="t/smoke/worker:dev",
    buildargs=SmokeWorkerContainer.buildargs(),
)

default_worker_container = container(
    image="{smoke_worker_image.id}",
    environment=fxtr("default_worker_env"),
    network="{DEFAULT_NETWORK.name}",
    volumes={"{default_worker_volume.name}": defaults.DEFAULT_WORKER_VOLUME},
    wrapper_class=SmokeWorkerContainer,
    timeout=defaults.DEFAULT_WORKER_CONTAINER_TIMEOUT,
)


@pytest.fixture
def default_worker_container_cls() -> Type[CeleryWorkerContainer]:
    return SmokeWorkerContainer


@pytest.fixture(scope="session")
def default_worker_container_session_cls() -> Type[CeleryWorkerContainer]:
    return SmokeWorkerContainer


@pytest.fixture
def default_worker_tasks() -> set:
    from t.smoke.common import tasks as common_tasks

    yield {
        common_tasks,
    }


# @pytest.fixture
# def smoke_worker(
#     smoke_worker_container: CeleryWorkerContainer,
#     celery_setup_app: Celery,
# ) -> CeleryTestWorker:
#     worker = CeleryTestWorker(
#         smoke_worker_container,
#         app=celery_setup_app,
#     )
#     yield worker


# smoke_worker_container = container(
#     image="{smoke_worker_image.id}",
#     environment=fxtr("default_worker_env"),
#     network="{DEFAULT_NETWORK.name}",
#     volumes={"{default_worker_volume.name}": defaults.DEFAULT_WORKER_VOLUME},
#     wrapper_class=SmokeWorkerContainer,
#     timeout=defaults.DEFAULT_WORKER_CONTAINER_TIMEOUT,
# )
