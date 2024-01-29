import os

import pytest
from pytest_celery import REDIS_CONTAINER_TIMEOUT, REDIS_ENV, REDIS_IMAGE, REDIS_PORTS, RedisContainer
from pytest_docker_tools import container, fetch, network

from t.smoke.operations.task_termination import TaskTermination
from t.smoke.operations.worker_kill import WorkerKill
from t.smoke.operations.worker_restart import WorkerRestart
from t.smoke.workers.alt import *  # noqa
from t.smoke.workers.dev import *  # noqa
from t.smoke.workers.latest import *  # noqa
from t.smoke.workers.other import *  # noqa


class SuiteOperations(
    TaskTermination,
    WorkerKill,
    WorkerRestart,
):
    pass


@pytest.fixture
def default_worker_tasks(default_worker_tasks: set) -> set:
    from t.integration import tasks as integration_tests_tasks
    from t.smoke import tasks as smoke_tests_tasks

    default_worker_tasks.add(integration_tests_tasks)
    default_worker_tasks.add(smoke_tests_tasks)
    return default_worker_tasks


redis_image = fetch(repository=REDIS_IMAGE)
redis_test_container_network = network(scope="session")
redis_test_container: RedisContainer = container(
    image="{redis_image.id}",
    scope="session",
    ports=REDIS_PORTS,
    environment=REDIS_ENV,
    network="{redis_test_container_network.name}",
    wrapper_class=RedisContainer,
    timeout=REDIS_CONTAINER_TIMEOUT,
)


@pytest.fixture(scope="session", autouse=True)
def set_redis_test_container(redis_test_container: RedisContainer):
    os.environ["REDIS_PORT"] = str(redis_test_container.port)
