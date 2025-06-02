from __future__ import annotations

import os

import pytest
from pytest_celery import (LOCALSTACK_CREDS, REDIS_CONTAINER_TIMEOUT, REDIS_ENV, REDIS_IMAGE, REDIS_PORTS,
                           CeleryTestSetup, RedisContainer)
from pytest_docker_tools import container, fetch, fxtr

from celery import Celery
from t.smoke.operations.task_termination import TaskTermination
from t.smoke.operations.worker_kill import WorkerKill
from t.smoke.operations.worker_restart import WorkerRestart
from t.smoke.workers.alt import *  # noqa
from t.smoke.workers.dev import *  # noqa
from t.smoke.workers.latest import *  # noqa
from t.smoke.workers.other import *  # noqa


class SmokeTestSetup(CeleryTestSetup):
    def ready(self, *args, **kwargs) -> bool:
        # Force false, false, true
        return super().ready(
            ping=False,
            control=False,
            docker=True,
        )


@pytest.fixture
def celery_setup_cls() -> type[CeleryTestSetup]:  # type: ignore
    return SmokeTestSetup


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

redis_command = RedisContainer.command()
redis_command.insert(1, "/usr/local/etc/redis/redis.conf")

redis_image = fetch(repository=REDIS_IMAGE)
redis_test_container: RedisContainer = container(
    image="{redis_image.id}",
    ports=REDIS_PORTS,
    environment=REDIS_ENV,
    network="{default_pytest_celery_network.name}",
    wrapper_class=RedisContainer,
    timeout=REDIS_CONTAINER_TIMEOUT,
    command=redis_command,
    volumes={
        os.path.abspath("t/smoke/redis.conf"): {
            "bind": "/usr/local/etc/redis/redis.conf",
            "mode": "ro",  # Mount as read-only
        }
    },
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


# Override the default redis broker container from pytest-celery
default_redis_broker = container(
    image="{default_redis_broker_image}",
    ports=fxtr("default_redis_broker_ports"),
    environment=fxtr("default_redis_broker_env"),
    network="{default_pytest_celery_network.name}",
    wrapper_class=RedisContainer,
    timeout=REDIS_CONTAINER_TIMEOUT,
    command=redis_command,
    volumes={
        os.path.abspath("t/smoke/redis.conf"): {
            "bind": "/usr/local/etc/redis/redis.conf",
            "mode": "ro",  # Mount as read-only
        }
    },
)


# Override the default redis backend container from pytest-celery
default_redis_backend = container(
    image="{default_redis_backend_image}",
    ports=fxtr("default_redis_backend_ports"),
    environment=fxtr("default_redis_backend_env"),
    network="{default_pytest_celery_network.name}",
    wrapper_class=RedisContainer,
    timeout=REDIS_CONTAINER_TIMEOUT,
    command=redis_command,
    volumes={
        os.path.abspath("t/smoke/redis.conf"): {
            "bind": "/usr/local/etc/redis/redis.conf",
            "mode": "ro",  # Mount as read-only
        }
    },
)
