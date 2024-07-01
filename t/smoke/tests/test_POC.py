from __future__ import annotations

import pytest
from pytest_celery import (RABBITMQ_CONTAINER_TIMEOUT, RABBITMQ_PORTS, RESULT_TIMEOUT, WORKER_DEBUGPY_PORTS,
                           CeleryBackendCluster, CeleryBrokerCluster, CeleryTestSetup, MemcachedTestBackend,
                           RabbitMQContainer, RabbitMQTestBroker)
from pytest_celery import sleep as sleeping
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


other_failover_broker = container(
    image="{default_rabbitmq_broker_image}",
    ports=fxtr("default_rabbitmq_broker_ports"),
    environment=fxtr("default_rabbitmq_broker_env"),
    network="{default_pytest_celery_network.name}",
    wrapper_class=RabbitMQContainer,
    timeout=RABBITMQ_CONTAINER_TIMEOUT,
)


@pytest.fixture
def broker3(other_failover_broker: RabbitMQContainer) -> RabbitMQManagementTestBroker:
    broker = RabbitMQManagementTestBroker(other_failover_broker)
    yield broker
    broker.teardown()


@pytest.fixture
def celery_broker_cluster(
    broker1: RabbitMQManagementTestBroker,
    broker2: RabbitMQManagementTestBroker,
    broker3: RabbitMQManagementTestBroker,
) -> CeleryBrokerCluster:
    cluster = CeleryBrokerCluster(broker1, broker2, broker3)
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
        initial_env = super().initial_env(
            celery_worker_cluster_config,
            # {
            #     "GEVENT_SUPPORT": True,
            #     "PYDEVD_DISABLE_FILE_VALIDATION": 1,
            # },
        )
        initial_env['CELERY_BROKER_URL'] = initial_env['CELERY_BROKER_URL'].replace(";", "|")
        # replace back only one | into ;
        # initial_env['CELERY_BROKER_URL'] = initial_env['CELERY_BROKER_URL'].replace("|", ";", 1)
        return initial_env


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
    app = default_worker_app
    app.conf.update(
        broker_use_ssl=False,
        broker_transport_options={"confirm_publish": True},
        worker_prefetch_multiplier=1,
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        task_result_expires=60 * 60 * 24,  # 24 hours
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


def test_publish_to_multiple_brokers(
    celery_setup: CeleryTestSetup,
    broker1: RabbitMQManagementTestBroker,
    broker2: RabbitMQManagementTestBroker,
    broker3: RabbitMQManagementTestBroker,
    publish_to_broker,
    subtests,
):
    with subtests.test(msg="Publish to broker1"):
        res: AsyncResult = publish_to_broker(broker1, identity.si("broker1"))

    with subtests.test(msg="Assert get() == 'broker1'"):
        assert res.get(timeout=RESULT_TIMEOUT) == "broker1"

    with subtests.test(msg="Publish to broker2"):
        res: AsyncResult = publish_to_broker(broker2, identity.si("broker2"))

    with subtests.test(msg="Assert get() == 'broker2'"):
        assert res.get(timeout=RESULT_TIMEOUT) == "broker2"

    with subtests.test(msg="Publish to broker3"):
        res: AsyncResult = publish_to_broker(broker3, identity.si("broker3"))

    with subtests.test(msg="Assert get() == 'broker3'"):
        assert res.get(timeout=RESULT_TIMEOUT) == "broker3"

    print("Done\n" + celery_setup.worker.logs())


def test_publish_large_traffic(
    celery_setup: CeleryTestSetup,
    broker1: RabbitMQManagementTestBroker,
    broker2: RabbitMQManagementTestBroker,
    broker3: RabbitMQManagementTestBroker,
    publish_to_broker,
    subtests,
):
    RESULT_TIMEOUT = 60 * 3
    count = 30
    brokers = 3
    # sig = group([identity.si(i) for i in range(count)])
    sleep_sig = group([sleeping.si(0.1) for i in range(count)])
    sig = sleep_sig

    with subtests.test(msg="Publish large traffic to broker1"):
        res: AsyncResult = publish_to_broker(broker1, sig)

    with subtests.test(msg="Assert get() == list(range(count))"):
        # assert res.get(timeout=RESULT_TIMEOUT) == list(range(count))
        assert all(res.get(timeout=RESULT_TIMEOUT))

    with subtests.test(msg="Publish large traffic to broker2"):
        res: AsyncResult = publish_to_broker(broker2, sig)

    with subtests.test(msg="Assert get() == list(range(count))"):
        # assert res.get(timeout=RESULT_TIMEOUT) == list(range(count))
        assert all(res.get(timeout=RESULT_TIMEOUT))

    with subtests.test(msg="Publish large traffic to broker3"):
        res: AsyncResult = publish_to_broker(broker3, sig)

    with subtests.test(msg="Assert get() == list(range(count))"):
        # assert res.get(timeout=RESULT_TIMEOUT) == list(range(count))
        assert all(res.get(timeout=RESULT_TIMEOUT))

    with subtests.test(msg="Publish to both brokers"):
        sig1 = group(identity.si(i) for i in range(count // brokers))
        sig2 = group(identity.si(i) for i in range(count // brokers, count))
        sig3 = group(identity.si(i) for i in range(count // brokers * 2, count))
        res1: AsyncResult = publish_to_broker(broker1, sig1)
        res2: AsyncResult = publish_to_broker(broker2, sig2)
        res3: AsyncResult = publish_to_broker(broker3, sig3)

    with subtests.test(msg="Assert get() == list(range(count // brokers))"):
        assert res1.get(timeout=RESULT_TIMEOUT) == list(range(count // brokers))

    with subtests.test(msg="Assert get() == list(range(count // brokers, count))"):
        assert res2.get(timeout=RESULT_TIMEOUT) == list(range(count // brokers, count))

    with subtests.test(msg="Assert get() == list(range(count // brokers * 2, count))"):
        assert res3.get(timeout=RESULT_TIMEOUT) == list(range(count // brokers * 2, count))

    print("Done\n" + celery_setup.worker.logs())


def test_kill_broker1(
    celery_setup: CeleryTestSetup,
    broker1: RabbitMQManagementTestBroker,
    broker2: RabbitMQManagementTestBroker,
    broker3: RabbitMQManagementTestBroker,
    publish_to_broker,
    subtests,
):
    count = 10
    sig = group([identity.si(i) for i in range(count)])

    with subtests.test(msg="Kill broker1"):
        broker1.kill()

    with subtests.test(msg="Publish large traffic to broker2"):
        res: AsyncResult = publish_to_broker(broker2, sig)

    with subtests.test(msg="Assert get() == list(range(count))"):
        assert res.get(timeout=RESULT_TIMEOUT) == list(range(count))

    with subtests.test(msg="Publish large traffic to broker3"):
        res: AsyncResult = publish_to_broker(broker3, sig)

    with subtests.test(msg="Assert get() == list(range(count))"):
        assert res.get(timeout=RESULT_TIMEOUT) == list(range(count))

    print("Done\n" + celery_setup.worker.logs())
