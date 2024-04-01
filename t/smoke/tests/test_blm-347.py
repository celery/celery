from __future__ import annotations

import pytest
from pytest_celery import (RABBITMQ_CONTAINER_TIMEOUT, RABBITMQ_PORTS, CeleryBackendCluster, CeleryBrokerCluster,
                           CeleryTestSetup, RabbitMQContainer, RabbitMQTestBroker, RedisTestBackend)
from pytest_docker_tools import build, container, fxtr

from celery import Celery
from celery.canvas import Signature  # noqa
from celery.result import AsyncResult  # noqa
from t.integration.tasks import identity
from t.smoke.workers.dev import SmokeWorkerContainer

###############################################################################
# RabbitMQ Management Broker
###############################################################################


class RabbitMQManagementTestBroker(RabbitMQTestBroker):
    def get_management_url(self) -> str:
        """Opening this link during debugging allows you to see the
        RabbitMQ management UI in your browser.
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
def broker1(
    default_rabbitmq_broker: RabbitMQContainer,
) -> RabbitMQManagementTestBroker:
    broker = RabbitMQManagementTestBroker(default_rabbitmq_broker)
    yield broker
    broker.teardown()


failover_broker = container(
    image="{default_rabbitmq_broker_image}",
    ports=fxtr("default_rabbitmq_broker_ports"),
    environment=fxtr("default_rabbitmq_broker_env"),
    network="{default_pytest_celery_network.name}",
    wrapper_class=RabbitMQContainer,
    timeout=RABBITMQ_CONTAINER_TIMEOUT,
)


@pytest.fixture
def broker2(failover_broker: RabbitMQContainer) -> RabbitMQManagementTestBroker:
    broker = RabbitMQManagementTestBroker(failover_broker)
    yield broker
    broker.teardown()


@pytest.fixture
def celery_broker_cluster(
    broker1: RabbitMQManagementTestBroker,
    broker2: RabbitMQManagementTestBroker,
) -> CeleryBrokerCluster:
    cluster = CeleryBrokerCluster(broker1, broker2)
    yield cluster
    cluster.teardown()


###############################################################################
# Redis Result Backend
###############################################################################


@pytest.fixture
def celery_backend_cluster(
    celery_redis_backend: RedisTestBackend,
) -> CeleryBackendCluster:
    cluster = CeleryBackendCluster(celery_redis_backend)
    yield cluster
    cluster.teardown()


@pytest.fixture
def default_redis_backend_image() -> str:
    return "redis:latest"


###############################################################################
# Worker Configuration
###############################################################################


class WorkerContainer(SmokeWorkerContainer):
    @classmethod
    def log_level(cls) -> str:
        return "INFO"

    @classmethod
    def worker_queue(cls) -> str:
        return "celery"

    @classmethod
    def command(cls, *args: str) -> list[str]:
        return super().command(
            "-Ofair",
            "--without-gossip",
            "--without-mingle",
            "--without-heartbeat",
            # "-P",
            # "gevent",
        )


@pytest.fixture
def default_worker_container_cls() -> type[SmokeWorkerContainer]:
    return WorkerContainer


@pytest.fixture(scope="session")
def default_worker_container_session_cls() -> type[SmokeWorkerContainer]:
    return WorkerContainer


celery_dev_worker_image = build(
    path=".",
    dockerfile="t/smoke/workers/docker/dev",
    tag="t/smoke/worker:dev",
    buildargs=WorkerContainer.buildargs(),
)


@pytest.fixture
def default_worker_app(default_worker_app: Celery) -> Celery:
    # Original environment configuration (including Bloomberg-specific 'dljson', removed in actual config)
    # CELERY_ACCEPT_CONTENT = ["json", "dljson"]
    # CELERY_TASK_SERIALIZER = "dljson"
    # CELERY_RESULT_SERIALIZER = "dljson"
    # CELERY_EVENT_SERIALIZER = "dljson"
    # BROKER_URL = get_broker_url(ssl=False)
    # BROKER_USE_SSL = False
    # BROKER_TRANSPORT_OPTIONS = {"confirm_publish": True}
    # BROKER_LOGIN_METHOD = "PLAIN"
    # CELERY_RESULT_BACKEND = "dbcache+bas://dlj2backendsvc-1.4"
    # CELERYD_PREFETCH_MULTIPLIER = 1  # If not set, each worker instance caches 4
    # CELERY_ACKS_LATE = True
    # CELERY_TASK_RESULT_EXPIRES = 60 * 60 * 24  # 24 hours
    # CELERY_TASK_PUBLISH_RETRY_POLICY = {
    #     "errback": kombu_error_handler,
    #     "max_retries": 20,
    #     "interval_start": 1,
    #     "interval_step": 2,
    #     "interval_max": 30,
    #     "retry_errors": (MessageNacked,),
    # }
    # CELERY_DEFAULT_DELIVERY_MODE = 1
    # CELERY_ENABLE_REMOTE_CONTROL = False

    app = default_worker_app
    app.conf.update(
        # Serialization formats
        accept_content=["json"],
        task_serializer="json",
        result_serializer="json",
        event_serializer="json",
        # Broker settings
        # broker_url=get_broker_url(ssl=False),
        broker_use_ssl=False,
        broker_transport_options={"confirm_publish": True},
        broker_login_method="PLAIN",
        # Result backend and worker settings
        # result_backend="dbcache+bas://dlj2backendsvc-1.4",
        worker_prefetch_multiplier=1,
        task_acks_late=True,
        # Task retry, expiration, and other settings
        task_result_expires=60 * 60 * 24,  # 24 hours
        task_publish_retry_policy={
            # "errback": kombu_error_handler,
            "max_retries": 20,
            "interval_start": 1,
            "interval_step": 2,
            "interval_max": 30,
            # "retry_errors": (MessageNacked,),
        },
        task_default_delivery_mode=1,
        worker_enable_remote_control=False,
    )

    return app


###############################################################################
# Publisher Side
###############################################################################


def test_blm_348(
    celery_setup: CeleryTestSetup,
    broker1: RabbitMQManagementTestBroker,
    broker2: RabbitMQManagementTestBroker,
):
    # Publish to broker1
    app1 = Celery(celery_setup.app.main)
    app1.conf = celery_setup.app.conf
    app1.conf["broker_url"] = broker1.config()["host_url"]
    assert identity.s("test_blm_348").apply_async(app=app1).get(timeout=5) == "test_blm_348"

    # Publish to broker2
    app2 = Celery(celery_setup.app.main)
    app2.conf = celery_setup.app.conf
    app2.conf["broker_url"] = broker2.config()["host_url"]
    assert identity.s("test_blm_348").apply_async(app=app1).get(timeout=5) == "test_blm_348"
