from __future__ import annotations

import pytest
from kombu import Exchange, Queue
from pytest_celery import CeleryBrokerCluster

from celery import Celery, uuid


@pytest.fixture
def celery_broker_cluster(request: pytest.FixtureRequest) -> CeleryBrokerCluster:
    broker = request.getfixturevalue(getattr(request, "param", "celery_rabbitmq_broker"))
    cluster = CeleryBrokerCluster(broker)
    yield cluster
    cluster.teardown()


@pytest.fixture
def celery_backend_cluster() -> None:
    return None


class test_task_exchange:
    def test_send_task_targets_default_direct_queue(self, celery_setup_app: Celery):
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
                assert message.delivery_info["exchange"] == ""
                assert message.delivery_info["routing_key"] == queue.name

    def test_send_task_uses_empty_queue_routing_key_with_explicit_exchange(self, celery_setup_app: Celery):
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
                    "tasks.add", args=(1, 2), queue=queue, exchange=exchange,
                    connection=connection, ignore_result=True,
                )

                message = bound_queue.get(no_ack=True)
                assert message is not None
                assert message.headers["id"] == result.id
                assert message.delivery_info["exchange"] == exchange.name
                assert message.delivery_info["routing_key"] == ""

    def test_send_task_unnamed_exchange_preserves_explicit_routing_key_with_queue(self, celery_setup_app: Celery):
        app = celery_setup_app
        queue = Queue(uuid(), Exchange(""), routing_key="rk_celery")
        target_queue = Queue(uuid(), Exchange(""))
        app.conf.update(broker_transport_options={"confirm_publish": True})

        with app.connection_for_write() as connection:
            with connection.channel() as channel:
                bound_queue = queue(channel)
                bound_queue.declare()
                bound_target_queue = target_queue(channel)
                bound_target_queue.declare()

                result = app.send_task(
                    "tasks.add", args=(1, 2), queue=queue, routing_key=target_queue.name,
                    connection=connection, ignore_result=True,
                )

                message = bound_target_queue.get(no_ack=True)
                assert message is not None
                assert message.headers["id"] == result.id
                assert message.delivery_info["exchange"] == ""
                assert message.delivery_info["routing_key"] == target_queue.name
                assert bound_queue.get(no_ack=True) is None

    @pytest.mark.parametrize(
        "celery_broker_cluster", ["celery_rabbitmq_broker", "celery_redis_broker"],
        indirect=True, ids=["rabbitmq", "redis"],
    )
    @pytest.mark.parametrize("use_default_queue", [False, True], ids=["explicit-queue", "default-queue"])
    def test_send_task_isolates_queues_with_shared_defaults(
        self, celery_setup_app: Celery, celery_broker_cluster: CeleryBrokerCluster, use_default_queue: bool,
    ):
        app = celery_setup_app
        queue_a, queue_b, default_queue = Queue(uuid()), Queue(uuid()), Queue(uuid())
        app.conf.update(
            broker_transport_options={"confirm_publish": True},
            task_queues=(queue_a, queue_b, default_queue),
            task_default_queue=default_queue.name,
            task_default_exchange=uuid(),
            task_default_routing_key="shared_key",
        )
        queues = [app.amqp.queues[queue.name] for queue in (queue_a, queue_b, default_queue)]
        for queue in queues:
            assert queue.exchange.name == app.conf.task_default_exchange
            assert queue.routing_key == "shared_key"
        target_queue = default_queue if use_default_queue else queue_a
        options = {} if use_default_queue else {"queue": target_queue.name}

        with app.connection_for_write() as connection:
            with connection.channel() as channel:
                bound_queues = [queue(channel) for queue in queues]
                for queue in bound_queues:
                    queue.declare()

                result = app.send_task(
                    "tasks.add", args=(1, 2), connection=connection, ignore_result=True, **options,
                )

                for queue in bound_queues:
                    message = queue.get(no_ack=True)
                    if queue.name == target_queue.name:
                        assert message is not None
                        assert message.headers["id"] == result.id
                        assert message.delivery_info["exchange"] == ""
                        assert message.delivery_info["routing_key"] == target_queue.name
                    else:
                        assert message is None

    def test_send_task_preserves_unnamed_exchange_object_with_named_producer(self, celery_setup_app: Celery):
        app = celery_setup_app
        app.conf.update(broker_transport_options={"confirm_publish": True})
        queue = Queue(uuid(), Exchange(""))
        producer_exchange = Exchange(uuid(), type="direct")
        other_queue = Queue(uuid(), producer_exchange, routing_key=queue.name)

        with app.connection_for_write() as connection:
            with connection.channel() as channel:
                bound_queue = queue(channel)
                bound_queue.declare()
                bound_other_queue = other_queue(channel)
                bound_other_queue.declare()
                producer = app.amqp.Producer(channel, exchange=producer_exchange)

                result = app.send_task(
                    "tasks.add", args=(1, 2), queue=queue, exchange=Exchange(""), routing_key=queue.name,
                    producer=producer, ignore_result=True,
                )

                message = bound_queue.get(no_ack=True)
                assert message is not None
                assert message.headers["id"] == result.id
                assert message.delivery_info["exchange"] == ""
                assert message.delivery_info["routing_key"] == queue.name
                assert bound_other_queue.get(no_ack=True) is None

    def test_send_task_unnamed_exchange_preserves_routing_key_without_queue(self, celery_setup_app: Celery):
        app = celery_setup_app
        routing_key = uuid()
        queue = Queue(routing_key, Exchange(""))
        app.conf.update(
            broker_transport_options={"confirm_publish": True},
            task_routes={"tasks.add": {"exchange": ""}},
        )

        with app.connection_for_write() as connection:
            with connection.channel() as channel:
                bound_queue = queue(channel)
                bound_queue.declare()

                result = app.send_task(
                    "tasks.add", args=(1, 2), routing_key=routing_key,
                    connection=connection, ignore_result=True,
                )

                message = bound_queue.get(no_ack=True)
                assert message is not None
                assert message.headers["id"] == result.id
                assert message.delivery_info["exchange"] == ""
                assert message.delivery_info["routing_key"] == routing_key

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
