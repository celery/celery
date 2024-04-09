from __future__ import annotations

import pytest
from pytest_celery import (RABBITMQ_CONTAINER_TIMEOUT, RABBITMQ_PORTS, RESULT_TIMEOUT, WORKER_DEBUGPY_PORTS,
                           CeleryBackendCluster, CeleryBrokerCluster, CeleryTestSetup, MemcachedTestBackend,
                           RabbitMQContainer, RabbitMQTestBroker)
from pytest_docker_tools import build, container, fxtr

from celery import Celery
from celery.canvas import Signature, group
from celery.result import AsyncResult
from t.integration.tasks import identity
from t.smoke.workers.dev import SmokeWorkerContainer

###############################################################################
# Broker
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
# Result Backend
###############################################################################


@pytest.fixture
def celery_backend_cluster(
    celery_memcached_backend: MemcachedTestBackend,
) -> CeleryBackendCluster:
    cluster = CeleryBackendCluster(celery_memcached_backend)
    yield cluster
    cluster.teardown()


###############################################################################
# Worker
###############################################################################


class WorkerContainer(SmokeWorkerContainer):
    @classmethod
    def log_level(cls) -> str:
        return "INFO"

    @classmethod
    def worker_queue(cls) -> str:
        return "celery"

    @classmethod
    def command(cls, *args: str, **kwargs: dict) -> list[str]:
        return super().command(
            "-Ofair",
            "--without-gossip",
            "--without-mingle",
            "--without-heartbeat",
            # "-P",
            # "gevent",
            debugpy=True,
            wait_for_client=False,
        )

    @classmethod
    def ports(cls) -> dict | None:
        return WORKER_DEBUGPY_PORTS

    @classmethod
    def initial_env(
        cls, celery_worker_cluster_config: dict, initial: dict | None = None
    ) -> dict:
        return super().initial_env(
            celery_worker_cluster_config,
            {
                "GEVENT_SUPPORT": True,
                "PYDEVD_DISABLE_FILE_VALIDATION": 1,
            },
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
        accept_content=["json"],
        task_serializer="json",
        result_serializer="json",
        event_serializer="json",
        # broker_url=get_broker_url(ssl=False),
        broker_use_ssl=False,
        broker_transport_options={"confirm_publish": True},
        broker_login_method="PLAIN",
        # result_backend="dbcache+bas://dlj2backendsvc-1.4",
        worker_prefetch_multiplier=1,
        task_acks_late=True,
        task_reject_on_worker_lost=True,
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
        worker_cancel_long_running_tasks_on_connection_loss=True,
    )

    return app


###############################################################################
# Publisher Side
###############################################################################


@pytest.fixture
def publish_to_broker():
    def _publish_to_broker(
        broker: RabbitMQManagementTestBroker,
        sig: Signature,
    ) -> AsyncResult:
        app = Celery(sig.app.main)
        app.conf = sig.app.conf
        app.conf["broker_url"] = broker.config()["host_url"]
        sig._app = app
        if isinstance(sig, group):
            for t in sig.tasks:
                t._app = app
        return sig.apply_async()

    return _publish_to_broker


def test_blm_348_publish_to_two_brokers(
    celery_setup: CeleryTestSetup,
    broker1: RabbitMQManagementTestBroker,
    broker2: RabbitMQManagementTestBroker,
    publish_to_broker,
):
    sig: Signature = identity.s("test_blm_348")

    # Publish to broker1
    res: AsyncResult = publish_to_broker(broker1, sig)
    assert res.get(timeout=RESULT_TIMEOUT) == "test_blm_348"

    # Publish to broker2
    res: AsyncResult = publish_to_broker(broker2, sig)
    assert res.get(timeout=RESULT_TIMEOUT) == "test_blm_348"

    print("Done\n" + celery_setup.worker.logs())
    pass


def test_blm_348_large_traffic(
    celery_setup: CeleryTestSetup,
    broker1: RabbitMQManagementTestBroker,
    broker2: RabbitMQManagementTestBroker,
    publish_to_broker,
):
    RESULT_TIMEOUT = 60 * 3
    count = 100
    sig = group([identity.s(i) for i in range(count)])

    # Publish large traffic to broker1
    res: AsyncResult = publish_to_broker(broker1, sig)
    assert res.get(timeout=RESULT_TIMEOUT) == list(range(count))

    # Publish large traffic to broker2
    res: AsyncResult = publish_to_broker(broker2, sig)
    assert res.get(timeout=RESULT_TIMEOUT) == list(range(count))

    # Publish to both brokers
    sig1 = group(identity.s(i) for i in range(count // 2))
    sig2 = group(identity.s(i) for i in range(count // 2, count))
    res1: AsyncResult = publish_to_broker(broker1, sig1)
    res2: AsyncResult = publish_to_broker(broker2, sig2)
    assert res1.get(timeout=RESULT_TIMEOUT) == list(range(count // 2))
    assert res2.get(timeout=RESULT_TIMEOUT) == list(range(count // 2, count))

    print("Done\n" + celery_setup.worker.logs())


def test_blm_348_broker2_only(
    celery_setup: CeleryTestSetup,
    broker1: RabbitMQManagementTestBroker,
    broker2: RabbitMQManagementTestBroker,
    publish_to_broker,
):
    count = 10
    sig = group([identity.s(i) for i in range(count)])

    # Publish large traffic to broker2
    broker1.kill()
    res: AsyncResult = publish_to_broker(broker2, sig)
    assert res.get(timeout=RESULT_TIMEOUT) == list(range(count))

    print("Done\n" + celery_setup.worker.logs())
