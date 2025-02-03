import pytest
from pytest_celery import (RABBITMQ_CONTAINER_TIMEOUT, RESULT_TIMEOUT, CeleryBrokerCluster, CeleryTestSetup,
                           RabbitMQContainer, RabbitMQTestBroker)
from pytest_docker_tools import container, fxtr

from t.integration.tasks import identity

failover_broker = container(
    image="{default_rabbitmq_broker_image}",
    ports=fxtr("default_rabbitmq_broker_ports"),
    environment=fxtr("default_rabbitmq_broker_env"),
    network="{default_pytest_celery_network.name}",
    wrapper_class=RabbitMQContainer,
    timeout=RABBITMQ_CONTAINER_TIMEOUT,
)


@pytest.fixture
def failover_rabbitmq_broker(failover_broker: RabbitMQContainer) -> RabbitMQTestBroker:
    broker = RabbitMQTestBroker(failover_broker)
    yield broker
    broker.teardown()


@pytest.fixture
def celery_broker_cluster(
    celery_rabbitmq_broker: RabbitMQTestBroker,
    failover_rabbitmq_broker: RabbitMQTestBroker,
) -> CeleryBrokerCluster:
    cluster = CeleryBrokerCluster(celery_rabbitmq_broker, failover_rabbitmq_broker)
    yield cluster
    cluster.teardown()


class test_broker_failover:
    def test_killing_first_broker(self, celery_setup: CeleryTestSetup):
        assert len(celery_setup.broker_cluster) > 1
        celery_setup.broker.kill()
        expected = "test_broker_failover"
        res = identity.s(expected).apply_async(queue=celery_setup.worker.worker_queue)
        assert res.get(timeout=RESULT_TIMEOUT) == expected

    def test_reconnect_to_main(self, celery_setup: CeleryTestSetup):
        assert len(celery_setup.broker_cluster) > 1
        celery_setup.broker_cluster[0].kill()
        expected = "test_broker_failover"
        res = identity.s(expected).apply_async(queue=celery_setup.worker.worker_queue)
        assert res.get(timeout=RESULT_TIMEOUT) == expected
        celery_setup.broker_cluster[1].kill()
        celery_setup.broker_cluster[0].restart()
        res = identity.s(expected).apply_async(queue=celery_setup.worker.worker_queue)
        assert res.get(timeout=RESULT_TIMEOUT) == expected

    def test_broker_failover_ui(self, celery_setup: CeleryTestSetup):
        assert len(celery_setup.broker_cluster) > 1
        celery_setup.broker_cluster[0].kill()
        celery_setup.worker.assert_log_exists("Will retry using next failover.")
        celery_setup.worker.assert_log_exists(
            f"Connected to amqp://guest:**@{celery_setup.broker_cluster[1].hostname()}:5672//"
        )
