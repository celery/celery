from __future__ import annotations

import pytest
from kombu import Producer, Queue
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


class test_task_queue_max_priority:
    @pytest.mark.parametrize("insertion_method", ["add", "setitem"])
    def test_messages_are_delivered_in_priority_order(
        self, celery_setup_app: Celery, insertion_method: str,
    ):
        app = celery_setup_app
        app.conf.broker_transport_options = {"confirm_publish": True}
        queues = Queues(max_priority=10)
        # The new queue should inherit the default max priority.
        queue = Queue(
            uuid(),
            queue_arguments={"x-queue-type": "classic"},
        )
        if insertion_method == "add":
            queues.add(queue)
        else:
            queues[queue.name] = queue

        with app.connection_for_write() as connection:
            with connection.channel() as channel:
                bound_queue = queues[queue.name](channel)
                bound_queue.declare()
                producer = Producer(channel, routing_key=queue.name)

                # Queue both messages before consuming so priority determines delivery order.
                producer.publish("low", priority=1)
                producer.publish("high", priority=9)

                assert bound_queue.get(no_ack=True).payload == "high"
                assert bound_queue.get(no_ack=True).payload == "low"
