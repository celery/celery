"""Smoke tests for quorum queue QoS behavior.

Verifies that tasks execute correctly on quorum queues with QoS settings
that differ from classic queues (no global QoS).

See https://github.com/celery/celery/issues/9929
"""
from __future__ import annotations

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup

from celery import Celery
from t.smoke.tasks import add


class test_quorum_qos:
    """Quorum queue task execution with QoS-related settings.

    Note: The original integration test (test_quorum_queue_qos_cluster_simulation)
    spawned 3 billiard processes to test multi-worker QoS race conditions. That
    scenario cannot be reproduced in the single-worker smoke test framework.
    These tests verify that QoS-related settings work correctly on quorum queues
    in a single-worker environment.
    """

    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.worker_prefetch_multiplier = 1
        app.conf.task_acks_late = True
        app.conf.task_reject_on_worker_lost = True
        return app

    def test_task_completes_on_quorum_queue(self, celery_setup: CeleryTestSetup):
        """Basic task execution succeeds on quorum queue with QoS settings."""
        queue = celery_setup.worker.worker_queue
        result = add.s(1, 2).set(queue=queue).delay()
        assert result.get(timeout=RESULT_TIMEOUT) == 3

    def test_multiple_tasks_complete(self, celery_setup: CeleryTestSetup):
        """Multiple concurrent tasks complete on quorum queue."""
        queue = celery_setup.worker.worker_queue
        results = [add.s(i, i).set(queue=queue).delay() for i in range(5)]
        for i, result in enumerate(results):
            assert result.get(timeout=RESULT_TIMEOUT) == i * 2
