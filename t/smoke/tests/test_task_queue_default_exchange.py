from __future__ import annotations

import pytest
from kombu import Exchange, Producer, Queue
from pytest_celery import CeleryBrokerCluster, RabbitMQTestBroker

from celery import Celery, uuid
from celery.app.amqp import Queues


@pytest.fixture
def celery_broker_cluster(celery_rabbitmq_broker: RabbitMQTestBroker) -> CeleryBrokerCluster:
    cluster = CeleryBrokerCluster(celery_rabbitmq_broker)
    yield cluster
    cluster.teardown()


@pytest.fixture
def celery_backend_cluster() -> None:
    return None


class test_task_queue_default_exchange:
    def test_queue_keeps_exchange_when_default_exchange_is_none(self, celery_setup_app: Celery):
        app = celery_setup_app
        app.conf.broker_transport_options = {"confirm_publish": True}
        exchange = Exchange("")
        # With no default exchange, the queue should keep its empty-name exchange.
        queues = Queues(default_exchange=None)
        queue = Queue(uuid(), exchange=exchange)
        queues.add(queue)

        with app.connection_for_write() as connection:
            with connection.channel() as channel:
                bound_queue = queues[queue.name](channel)
                bound_queue.declare()
                producer = Producer(channel, exchange=exchange, routing_key=queue.name)
                producer.publish("message")

                message = bound_queue.get(no_ack=True)
                assert message is not None
                assert message.payload == "message"
