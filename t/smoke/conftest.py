import os
from typing import Tuple, Type

import pytest
from pytest_celery import CeleryTestWorker, CeleryWorkerCluster, CeleryWorkerContainer, defaults
from pytest_docker_tools import build, container, fxtr

from celery import Celery
from t.smoke.workers import Celery4WorkerContainer, CeleryLatestWorkerContainer, SmokeWorkerContainer

# Celery 4.x

celery4_worker_image = build(
    path=".",
    dockerfile="t/smoke/workers/pypi",
    tag="t/smoke/worker:celery4",
    buildargs=Celery4WorkerContainer.buildargs(),
)


celery4_worker_container = container(
    image="{celery4_worker_image.id}",
    environment=fxtr("default_worker_env"),
    network="{default_pytest_celery_network.name}",
    volumes={"{default_worker_volume.name}": defaults.DEFAULT_WORKER_VOLUME},
    wrapper_class=Celery4WorkerContainer,
    timeout=defaults.DEFAULT_WORKER_CONTAINER_TIMEOUT,
)


@pytest.fixture
def celery4_worker(
    celery4_worker_container: Celery4WorkerContainer,
    celery_setup_app: Celery,
) -> CeleryTestWorker:
    yield CeleryTestWorker(celery4_worker_container, app=celery_setup_app)


# Celery Latest (last officially released version)

celery_latest_worker_image = build(
    path=".",
    dockerfile="t/smoke/workers/latest",
    tag="t/smoke/worker:latest",
    buildargs=CeleryLatestWorkerContainer.buildargs(),
)


celery_latest_worker_container = container(
    image="{celery_latest_worker_image.id}",
    environment=fxtr("default_worker_env"),
    network="{default_pytest_celery_network.name}",
    volumes={"{default_worker_volume.name}": defaults.DEFAULT_WORKER_VOLUME},
    wrapper_class=CeleryLatestWorkerContainer,
    timeout=defaults.DEFAULT_WORKER_CONTAINER_TIMEOUT,
)


@pytest.fixture
def celery_latest_worker(
    celery_latest_worker_container: CeleryLatestWorkerContainer,
    celery_setup_app: Celery,
) -> CeleryTestWorker:
    yield CeleryTestWorker(celery_latest_worker_container, app=celery_setup_app)


# Dev worker that overrides the default plugin worker

celery_dev_worker_image = build(
    path=".",
    dockerfile="t/smoke/workers/dev",
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


# Tasks


@pytest.fixture
def default_worker_tasks() -> set:
    from t.integration import tasks as integration_tests_tasks
    from t.smoke import tasks as smoke_tests_tasks

    yield {
        integration_tests_tasks,
        smoke_tests_tasks,
    }


# Workers cluster


@pytest.fixture(
    # Each param item is a list of workers to be used in the cluster
    params=[
        ["celery_setup_worker"],
        ["celery_setup_worker", "celery4_worker"],
        ["celery_setup_worker", "celery_latest_worker"],
        ["celery_setup_worker", "celery4_worker", "celery_latest_worker"],
    ]
)
def celery_worker_cluster(request: pytest.FixtureRequest) -> CeleryWorkerCluster:
    nodes: Tuple[CeleryTestWorker] = [request.getfixturevalue(worker) for worker in request.param]
    cluster = CeleryWorkerCluster(*nodes)
    yield cluster
    cluster.teardown()
