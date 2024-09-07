import os

import pytest
from pytest_celery import (LOCALSTACK_CREDS, REDIS_CONTAINER_TIMEOUT, REDIS_ENV, REDIS_IMAGE, REDIS_PORTS,
                           RedisContainer)
from pytest_docker_tools import container, fetch

import docker
from celery import Celery
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
    """Optional operations that can be performed with different methods,
    shared across the smoke tests suite.

    Example Usage:
    >>> class test_mysuite(SuiteOperations):
    >>>     def test_something(self):
    >>>         self.prepare_worker_with_conditions()
    >>>         assert condition are met
    """


@pytest.fixture
def default_worker_tasks(default_worker_tasks: set) -> set:
    """Use all of the integration and smoke suites tasks in the smoke tests workers."""
    from t.integration import tasks as integration_tests_tasks
    from t.smoke import tasks as smoke_tests_tasks

    default_worker_tasks.add(integration_tests_tasks)
    default_worker_tasks.add(smoke_tests_tasks)
    return default_worker_tasks


# When using integration tests tasks that requires a Redis instance,
# we use pytest-celery to raise a dedicated Redis container for the smoke tests suite that is configured
# to be used by the integration tests tasks.

redis_image = fetch(repository=REDIS_IMAGE)
redis_test_container: RedisContainer = container(
    image="{redis_image.id}",
    ports=REDIS_PORTS,
    environment=REDIS_ENV,
    network="{default_pytest_celery_network.name}",
    wrapper_class=RedisContainer,
    timeout=REDIS_CONTAINER_TIMEOUT,
)


@pytest.fixture(autouse=True)
def set_redis_test_container(redis_test_container: RedisContainer):
    """Configure the Redis test container to be used by the integration tests tasks."""
    # get_redis_connection(): will use these settings in the tests environment
    os.environ["REDIS_HOST"] = "localhost"
    os.environ["REDIS_PORT"] = str(redis_test_container.port)


@pytest.fixture
def default_worker_env(default_worker_env: dict, redis_test_container: RedisContainer) -> dict:
    """Add the Redis connection details to the worker environment."""
    # get_redis_connection(): will use these settings when executing tasks in the worker
    default_worker_env.update(
        {
            "REDIS_HOST": redis_test_container.hostname,
            "REDIS_PORT": 6379,
            **LOCALSTACK_CREDS,
        }
    )
    return default_worker_env


@pytest.fixture(scope="session", autouse=True)
def set_aws_credentials():
    os.environ.update(LOCALSTACK_CREDS)


@pytest.fixture
def default_worker_app(default_worker_app: Celery) -> Celery:
    app = default_worker_app
    if app.conf.broker_url and app.conf.broker_url.startswith("sqs"):
        app.conf.broker_transport_options["region"] = LOCALSTACK_CREDS["AWS_DEFAULT_REGION"]
    return app


@pytest.fixture(scope="module", autouse=True)
def auto_clean_docker_resources():
    """Clean up Docker resources after each test module."""
    # Used for debugging
    verbose = False

    def log(message):
        if verbose:
            print(message)

    def cleanup_docker_resources():
        """Function to clean up Docker containers, networks, and volumes based on labels."""
        docker_client = docker.from_env()

        try:
            # Clean up containers with the label 'creator=pytest-docker-tools'
            containers = docker_client.containers.list(all=True, filters={"label": "creator=pytest-docker-tools"})
            for con in containers:
                con.reload()  # Ensure we have the latest status
                if con.status != "running":  # Only remove non-running containers
                    log(f"Removing container {con.name}")
                    con.remove(force=True)
                else:
                    log(f"Skipping running container {con.name}")

            # Clean up networks with names starting with 'pytest-'
            networks = docker_client.networks.list(names=["pytest-*"])
            for network in networks:
                if not network.containers:  # Check if the network is in use
                    log(f"Removing network {network.name}")
                    network.remove()
                else:
                    log(f"Skipping network {network.name}, still in use")

            # Clean up volumes with names starting with 'pytest-*'
            volumes = docker_client.volumes.list(filters={"name": "pytest-*"})
            for volume in volumes:
                if not volume.attrs.get("UsageData", {}).get("RefCount", 0):  # Check if volume is not in use
                    log(f"Removing volume {volume.name}")
                    volume.remove()
                else:
                    log(f"Skipping volume {volume.name}, still in use")

        except Exception as e:
            log(f"Error occurred while cleaning up Docker resources: {e}")

    log("--- Running Docker resource cleanup ---")
    cleanup_docker_resources()
