"""Smoke tests for RabbitMQ cycle detection with quorum queues.

Reproduces conditions for https://github.com/celery/celery/issues/9867
when running against RabbitMQ <4.0.1.
"""
from __future__ import annotations

from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup

from t.smoke.tasks import countdown_task_with_retry, non_eta_task_with_retries


class test_cycle_detection:
    """Verify countdown retries don't trigger RabbitMQ cycle detection."""

    def test_countdown_task_with_retry(self, celery_setup: CeleryTestSetup):
        """Task with countdown=3 retried once with same delay."""
        queue = celery_setup.worker.worker_queue
        sig = countdown_task_with_retry.s().set(queue=queue)
        assert sig.apply_async(countdown=3).get(timeout=RESULT_TIMEOUT) == 1

    def test_non_eta_task_with_multiple_retries(self, celery_setup: CeleryTestSetup):
        """Task retried twice with countdown=3 each time."""
        queue = celery_setup.worker.worker_queue
        sig = non_eta_task_with_retries.s().set(queue=queue)
        assert sig.apply_async().get(timeout=RESULT_TIMEOUT) == 2
