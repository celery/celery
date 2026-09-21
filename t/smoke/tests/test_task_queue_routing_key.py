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


class test_task_queue_routing_key:
    @pytest.mark.parametrize("insertion_method", ["add", "setitem"])
    def test_queue_receives_messages_using_default_routing_key(
        self, celery_setup_app: Celery, insertion_method: str,
    ):
        app = celery_setup_app
        app.conf.broker_transport_options = {"confirm_publish": True}
        queues = Queues(default_routing_key="tasks.default")
        exchange = Exchange(uuid(), type="topic")
        # The new queue should inherit the default routing key.
        queue = Queue(uuid(), exchange=exchange)
        if insertion_method == "add":
            queues.add(queue)
        else:
            queues[queue.name] = queue

        with app.connection_for_write() as connection:
            with connection.channel() as channel:
                bound_queue = queues[queue.name](channel)
                bound_queue.declare()
                producer = Producer(channel, exchange=exchange, routing_key="tasks.default")
                producer.publish("message")

                message = bound_queue.get(no_ack=True)
                assert message is not None
                assert message.payload == "message"
