from __future__ import annotations

import os

import pytest
from pytest_celery import RABBITMQ_PORTS, CeleryBrokerCluster, RabbitMQContainer, RabbitMQTestBroker, defaults
from pytest_docker_tools import build, container, fxtr

from celery import Celery
from t.smoke.workers.dev import SmokeWorkerContainer

###############################################################################
# RabbitMQ Management Broker
###############################################################################


class RabbitMQManagementBroker(RabbitMQTestBroker):
    def get_management_url(self) -> str:
        """Opening this link during debugging allows you to see the
        RabbitMQ management UI in your browser.

        Usage from a test:
        >>> celery_setup.broker.get_management_url()

        Open from a browser and login with guest:guest.
        """
        ports = self.container.attrs["NetworkSettings"]["Ports"]
        ip = ports["15672/tcp"][0]["HostIp"]
        port = ports["15672/tcp"][0]["HostPort"]
        return f"http://{ip}:{port}"


@pytest.fixture
def default_rabbitmq_broker_image() -> str:
    return "rabbitmq:management"


@pytest.fixture
def default_rabbitmq_broker_ports() -> dict:
    # Expose the management UI port
    ports = RABBITMQ_PORTS.copy()
    ports.update({"15672/tcp": None})
    return ports


@pytest.fixture
def celery_rabbitmq_broker(default_rabbitmq_broker: RabbitMQContainer) -> RabbitMQTestBroker:
    broker = RabbitMQManagementBroker(default_rabbitmq_broker)
    yield broker
    broker.teardown()


@pytest.fixture
def celery_broker_cluster(celery_rabbitmq_broker: RabbitMQTestBroker) -> CeleryBrokerCluster:
    cluster = CeleryBrokerCluster(celery_rabbitmq_broker)
    yield cluster
    cluster.teardown()


###############################################################################
# Worker Configuration
###############################################################################


class QuorumWorkerContainer(SmokeWorkerContainer):
    @classmethod
    def log_level(cls) -> str:
        return "INFO"

    @classmethod
    def worker_queue(cls) -> str:
        return "celery"


@pytest.fixture
def default_worker_container_cls() -> type[SmokeWorkerContainer]:
    return QuorumWorkerContainer


@pytest.fixture(scope="session")
def default_worker_container_session_cls() -> type[SmokeWorkerContainer]:
    return QuorumWorkerContainer


celery_dev_worker_image = build(
    path=".",
    dockerfile="t/smoke/workers/docker/dev",
    tag="t/smoke/worker:dev",
    buildargs=QuorumWorkerContainer.buildargs(),
)


default_worker_container = container(
    image="{celery_dev_worker_image.id}",
    ports=fxtr("default_worker_ports"),
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
    wrapper_class=QuorumWorkerContainer,
    timeout=defaults.DEFAULT_WORKER_CONTAINER_TIMEOUT,
    command=fxtr("default_worker_command"),
)


@pytest.fixture
def default_worker_app(default_worker_app: Celery) -> Celery:
    app = default_worker_app
    app.conf.broker_transport_options = {"confirm_publish": True}
    app.conf.task_default_queue_type = "quorum"

    return app
