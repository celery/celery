from __future__ import annotations

import pytest
from kombu import Exchange, Queue
from pytest_celery import CeleryBrokerCluster, RabbitMQTestBroker

from celery import Celery, uuid


@pytest.fixture
def celery_broker_cluster(celery_rabbitmq_broker: RabbitMQTestBroker) -> CeleryBrokerCluster:
    cluster = CeleryBrokerCluster(celery_rabbitmq_broker)
    yield cluster
    cluster.teardown()


@pytest.fixture
def celery_backend_cluster() -> None:
    return None


class test_task_exchange:
    def test_send_task_uses_configured_direct_exchange(self, celery_setup_app: Celery):
        app = celery_setup_app
        exchange = Exchange(uuid(), type="direct")
        queue = Queue(uuid(), exchange=exchange, routing_key="rk_celery")
        app.conf.update(
            broker_transport_options={"confirm_publish": True},
            task_queues=(queue,),
            task_default_queue=queue.name,
            task_default_exchange=uuid(),
            task_default_routing_key="default_rk_celery",
        )

        with app.connection_for_write() as connection:
            with connection.channel() as channel:
                bound_queue = queue(channel)
                bound_queue.declare()

                result = app.send_task(
                    "tasks.add", args=(1, 2), connection=connection, ignore_result=True,
                )

                message = bound_queue.get(no_ack=True)
                assert message is not None
                assert message.headers["id"] == result.id
                assert message.delivery_info["exchange"] == exchange.name
                assert message.delivery_info["routing_key"] == "rk_celery"

    def test_send_task_uses_empty_queue_routing_key(self, celery_setup_app: Celery):
        app = celery_setup_app
        app.conf.update(
            broker_transport_options={"confirm_publish": True},
            task_default_routing_key="default_rk_celery",
        )
        exchange = Exchange(uuid(), type="direct")
        queue = Queue(uuid(), exchange=exchange, routing_key="")

        with app.connection_for_write() as connection:
            with connection.channel() as channel:
                bound_queue = queue(channel)
                bound_queue.declare()

                result = app.send_task(
                    "tasks.add", args=(1, 2), queue=queue,
                    connection=connection, ignore_result=True,
                )

                message = bound_queue.get(no_ack=True)
                assert message is not None
                assert message.headers["id"] == result.id
                assert message.delivery_info["exchange"] == exchange.name
                assert message.delivery_info["routing_key"] == ""

    @pytest.mark.parametrize("exchange_as_string", [True, False], ids=["string", "exchange-object"])
    def test_send_task_uses_explicit_exchange_without_routing_key(
        self, celery_setup_app: Celery, exchange_as_string: bool,
    ):
        app = celery_setup_app
        exchange = Exchange(uuid(), type="direct")
        queue = Queue(uuid(), exchange=exchange, routing_key="rk_celery")
        app.conf.update(
            broker_transport_options={"confirm_publish": True},
            task_default_queue=uuid(),
            task_default_exchange=uuid(),
            task_default_routing_key="rk_celery",
        )

        with app.connection_for_write() as connection:
            with connection.channel() as channel:
                bound_queue = queue(channel)
                bound_queue.declare()

                result = app.send_task(
                    "tasks.add", args=(1, 2), connection=connection, ignore_result=True,
                    exchange=exchange.name if exchange_as_string else exchange,
                )

                message = bound_queue.get(no_ack=True)
                assert message is not None
                assert message.headers["id"] == result.id
                assert message.delivery_info["exchange"] == exchange.name
                assert message.delivery_info["routing_key"] == "rk_celery"
