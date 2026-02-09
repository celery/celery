"""
Integration tests for RabbitMQ cycle detection bug with quorum queues.

This reproduces the conditions for
https://github.com/celery/celery/issues/9867 when running against RabbitMQ
<4.0.1.
"""

import os

import pytest
from kombu import Exchange, Queue

from celery import Celery, exceptions
from celery.contrib.testing.worker import start_worker


class TaskFailedException(Exception):
    """Test exception for tasks that should fail and retry."""
    pass


def get_rabbitmq_url():
    """Get RabbitMQ broker URL from environment."""
    user = os.environ.get("RABBITMQ_DEFAULT_USER", "guest")
    password = os.environ.get("RABBITMQ_DEFAULT_PASSWORD", "guest")
    return os.environ.get(
        "TEST_BROKER", f"pyamqp://{user}:{password}@localhost:5672//")


def get_backend_url():
    """Get backend URL from environment."""
    return os.environ.get("TEST_BACKEND", "rpc")


def create_test_app_with_quorum_queues():
    """
    Create Celery app configured for quorum queue testing.

    Returns:
        Tuple of (app, queue_name)
    """
    broker_url = get_rabbitmq_url()
    backend_url = get_backend_url()

    app = Celery(
        "test_cycle_detection",
        broker=broker_url,
        backend=backend_url,
    )

    # Configure queue with quorum queue type
    queue_name = 'test_cycle_queue'
    default_exchange = Exchange('celery.topic', type='topic')

    app.conf.task_queues = [
        Queue(
            queue_name,
            exchange=default_exchange,
            routing_key=queue_name,
            queue_arguments={'x-queue-type': 'quorum'}
        ),
    ]

    app.conf.task_default_queue = queue_name

    # Recommended settings for quorum queues
    app.conf.broker_transport_options = {"confirm_publish": True}

    # Enable result extended to track retries
    app.conf.result_extended = True

    return app


@pytest.mark.amqp
@pytest.mark.timeout(35)
def test_countdown_task_with_retry():
    """Test where a countdown task gets retried with the same delay."""
    app = create_test_app_with_quorum_queues()

    @app.task(bind=True, default_retry_delay=3, max_retries=1)
    def countdown_task_with_retry(self):
        """Task that fails once then succeeds."""
        # First attempt: fail to trigger retry
        if self.request.retries < 1:
            raise self.retry(exc=TaskFailedException("Simulated failure"))

        return self.request.retries

    with start_worker(
        app,
        loglevel="INFO",
        shutdown_timeout=15,
    ):
        # Execute task with countdown=3 (same as default_retry_delay)
        # We need to hit the same delayed buckets to trigger the cycle detection
        # in RabbitMQ.
        result = countdown_task_with_retry.apply_async(countdown=3)

        try:
            # Wait for the task to complete, we must wait at least long enough
            # for the countdown to be reached and the retry to occur (6s).
            value = result.get(timeout=15)
        except exceptions.TimeoutError as e:
            pytest.fail(
                f"Task was silently dropped by RabbitMQ due to cycle detection. "
                f"Exception: {e!r}."
            )

        assert value == 1, \
            f"Expected task to succeed after 1 retry, got: {value}"


@pytest.mark.amqp
@pytest.mark.timeout(35)
def test_non_eta_task_with_multiple_retries():
    """Test where a task gets retried twice with the same delay."""
    app = create_test_app_with_quorum_queues()

    @app.task(bind=True, max_retries=2)
    def non_eta_task_with_retries(self):
        """Task that fails twice then succeeds."""
        # First and second attempts: fail to trigger retry
        if self.request.retries < 2:
            raise self.retry(exc=TaskFailedException("Simulated failure"), countdown=3)

        return self.request.retries

    with start_worker(
        app,
        loglevel="INFO",
        shutdown_timeout=15,
    ):
        result = non_eta_task_with_retries.apply_async()

        try:
            # Wait for the task to complete, we must wait at least long enough
            # for the two retries to occur (6s).
            value = result.get(timeout=15)
        except exceptions.TimeoutError as e:
            pytest.fail(
                f"Task was silently dropped by RabbitMQ due to cycle detection. "
                f"Exception: {e!r}"
            )

        assert value == 2, \
            f"Expected task to succeed after 2 retries, got: {value}"
