"""Integration tests for native delayed delivery queue binding.

Tests that verify queue bindings are created correctly for native delayed
delivery, especially when some queues in task_queues fail to bind.
"""
import os
import uuid
from urllib.parse import quote

import pytest
import requests
from kombu import Exchange, Queue
from requests.auth import HTTPBasicAuth

from celery import Celery
from celery.contrib.testing.worker import start_worker


def get_rabbitmq_credentials():
    """Get RabbitMQ credentials from environment."""
    user = os.environ.get("RABBITMQ_DEFAULT_USER", "guest")
    password = os.environ.get("RABBITMQ_DEFAULT_PASSWORD", "guest")
    return user, password


def get_rabbitmq_url():
    """Get RabbitMQ broker URL from environment."""
    user, password = get_rabbitmq_credentials()
    return os.environ.get(
        "TEST_BROKER", f"pyamqp://{user}:{password}@localhost:5672//")


def get_management_api_url():
    """Get RabbitMQ Management API base URL."""
    return "http://localhost:15672/api"


def get_bindings_for_exchange(exchange_name, vhost='/'):
    """Fetch bindings where the given exchange is the source.

    Args:
        exchange_name: Name of the exchange
        vhost: Virtual host (default '/')

    Returns:
        List of binding dictionaries
    """
    user, password = get_rabbitmq_credentials()
    vhost_encoded = quote(vhost, safe='')
    exchange_encoded = quote(exchange_name, safe='')
    api_url = (
        f"{get_management_api_url()}/exchanges/{vhost_encoded}/"
        f"{exchange_encoded}/bindings/source"
    )
    response = requests.get(api_url, auth=HTTPBasicAuth(user, password))
    response.raise_for_status()
    return response.json()


def get_bindings_for_queue(queue_name, vhost='/'):
    """Fetch bindings for a specific queue.

    Args:
        queue_name: Name of the queue
        vhost: Virtual host (default '/')

    Returns:
        List of binding dictionaries
    """
    user, password = get_rabbitmq_credentials()
    vhost_encoded = quote(vhost, safe='')
    queue_encoded = quote(queue_name, safe='')
    api_url = (
        f"{get_management_api_url()}/queues/{vhost_encoded}/{queue_encoded}/"
        "bindings"
    )
    response = requests.get(api_url, auth=HTTPBasicAuth(user, password))
    response.raise_for_status()
    return response.json()


def create_test_app(unique_id):
    """Create Celery app configured for native delayed delivery testing.

    Args:
        unique_id: Unique identifier to ensure queue/exchange names don't
                   conflict

    Returns:
        Tuple of (app, exchange_name, queue_a_name, queue_b_name)
    """
    broker_url = get_rabbitmq_url()

    # Get Redis backend URL from environment
    redis_host = os.environ.get("REDIS_HOST", "localhost")
    redis_port = os.environ.get("REDIS_PORT", "6379")
    backend_url = os.environ.get(
        "TEST_BACKEND", f"redis://{redis_host}:{redis_port}/0")

    app = Celery(
        "test_native_delayed_delivery_binding",
        broker=broker_url,
        backend=backend_url,
    )

    # Configure topic exchange with unique name
    exchange_name = f'celery.topic_{unique_id}'
    default_exchange = Exchange(exchange_name, type='topic')

    # Define task queues with queue-a first, queue-b second
    queue_a_name = f'queue-a_{unique_id}'
    queue_b_name = f'queue-b_{unique_id}'
    app.conf.task_queues = [
        Queue(queue_a_name, exchange=default_exchange,
              routing_key=queue_a_name,
              queue_arguments={'x-queue-type': 'quorum'}),
        Queue(queue_b_name, exchange=default_exchange,
              routing_key=queue_b_name,
              queue_arguments={'x-queue-type': 'quorum'}),
    ]

    # Recommended setting for using celery with quorum queues
    app.conf.broker_transport_options = {"confirm_publish": True}

    # Enable quorum queue detection to disable global QoS
    app.conf.worker_detect_quorum_queues = True

    return app, exchange_name, queue_a_name, queue_b_name


@pytest.mark.amqp
@pytest.mark.timeout(90)
def test_worker_binds_consumed_queue_despite_earlier_queue_failure():
    """Test that queue binding continues even when earlier queues fail to bind.

    This test reproduces the scenario from
    https://github.com/celery/celery/issues/9960
    """
    unique_id = uuid.uuid4().hex
    app, exchange_name, queue_a_name, queue_b_name = create_test_app(unique_id)

    # Set default queue to queue-b so the start_worker ping task is received
    # by our worker
    app.conf.task_default_queue = queue_b_name

    # Start worker that only consumes from queue-b
    # queue-a is NOT consumed, so it won't be declared by this worker
    with start_worker(
        app,
        queues=[queue_b_name],
        loglevel="INFO",
        perform_ping_check=True,
        shutdown_timeout=15,
    ):
        # Check celery_delayed_delivery → exchange bindings
        delayed_delivery_bindings = \
            get_bindings_for_exchange('celery_delayed_delivery')
        queue_b_delayed_binding = [
            b for b in delayed_delivery_bindings
            if b.get('destination') == exchange_name
            and b.get('routing_key') == f'#.{queue_b_name}'
        ]
        assert len(queue_b_delayed_binding) >= 1, (
            f"Expected delayed delivery binding for {queue_b_name!r}, but "
            f"got bindings: {delayed_delivery_bindings!r}"
        )

        # Check celery.topic → queue-b bindings
        # Should have bindings from the topic exchange to queue-b for both
        # immediate and delayed delivery
        queue_b_bindings = get_bindings_for_queue(queue_b_name)
        topic_to_queue_bindings = [
            b for b in queue_b_bindings
            if b.get('source') == exchange_name
        ]
        topic_to_queue_routing_keys = {
            b.get('routing_key') for b in topic_to_queue_bindings
        }

        # Check the routing key for immediate delivery
        assert queue_b_name in topic_to_queue_routing_keys, (
            f"Expected routing key {queue_b_name!r} in bindings, but got: "
            f"{topic_to_queue_bindings!r}"
        )

        # Check the routing key for delayed delivery
        assert f"#.{queue_b_name}" in topic_to_queue_routing_keys, (
            f"Expected at least one binding from {exchange_name!r} to "
            f"{queue_b_name!r}, but got: {topic_to_queue_bindings!r}"
        )
