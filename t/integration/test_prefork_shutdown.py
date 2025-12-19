"""Integration tests for prefork pool shutdown behaviour.

These tests verify that the prefork pool gracefully shuts down and maintains
heartbeats during the shutdown process, preventing connection loss during
worker drain.
"""

from time import sleep

import pytest

from celery.contrib.testing.worker import start_worker
from celery.worker import state

from .tasks import sleeping

TEST_HEARTBEAT = 2

# Exceeds AMQP connection timeout (~4 seconds: broker closes after missing
# 2 consecutive 2-second heartbeats)
LONG_TASK_DURATION = 10

TIMEOUT = LONG_TASK_DURATION * 2


@pytest.fixture
def heartbeat_worker(celery_session_app):
    """Worker with short heartbeat for testing purposes."""

    # Temporarily lower heartbeat for this test
    original_heartbeat = celery_session_app.conf.broker_heartbeat
    celery_session_app.conf.broker_heartbeat = TEST_HEARTBEAT

    original_acks_late = celery_session_app.conf.task_acks_late
    celery_session_app.conf.task_acks_late = True

    with start_worker(
        celery_session_app,
        pool="prefork",
        without_heartbeat=False,
        concurrency=4,
        shutdown_timeout=TIMEOUT,
        perform_ping_check=False,
    ) as worker:
        # Verify that low heartbeat is configured correctly
        assert worker.consumer.amqheartbeat == TEST_HEARTBEAT

        yield worker

    celery_session_app.conf.broker_heartbeat = original_heartbeat
    celery_session_app.conf.task_acks_late = original_acks_late


class test_prefork_shutdown:
    """Test prefork shutdown with heartbeat maintenance."""

    # Test timeout should be longer than worker timeout
    @pytest.mark.timeout(timeout=TIMEOUT * 2)
    @pytest.mark.usefixtures("heartbeat_worker")
    def test_shutdown_with_long_running_tasks(self):
        """Test that graceful shutdown completes long-running tasks without
        connection loss.

        This test verifies that when the prefork pool is shutting down with
        long-running tasks, heartbeats continue to be sent to maintain the
        broker connection.

        - Heartbeat frames sent every 2 seconds
        - Connection closes after 4 seconds (two missed frames) without heartbeats
        - Tasks run 10 seconds to exceed 4-second threshold
        """

        # Submit multiple long-running tasks that will be active during shutdown
        num_tasks = 3
        results = []
        for _ in range(num_tasks):
            results.append(sleeping.delay(LONG_TASK_DURATION))

        # Give time for tasks to start executing
        sleep(1)

        state.should_stop = True

        # Wait for all tasks to complete. If heartbeats aren't maintained during
        # shutdown, this will fail with `ConnectionResetError`, `BrokenPipeError`
        # and `celery.exceptions.TimeoutError`.
        for result in results:
            result.get(timeout=TIMEOUT)
            assert result.status == "SUCCESS"
