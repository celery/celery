"""Smoke tests for quorum queue QoS behavior.

Verifies that tasks execute correctly on quorum queues with QoS settings
that differ from classic queues (no global QoS).

See https://github.com/celery/celery/issues/9929
"""
from __future__ import annotations

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup

from celery import Celery
from celery.canvas import group
from t.smoke.tasks import add, long_running_task, noop


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


class test_quorum_qos_prefetch_reduction_skipped_on_reconnect:
    """Regression tests for celery/celery#9512.

    On quorum queues (per-consumer QoS, ``apply_global=False``),
    ``basic.qos`` updates do not propagate to already-running consumers.
    Celery's prefetch reduction mechanism, which reduces the prefetch
    count on connection loss and gradually restores it via ``basic.qos``
    on each task ack, is therefore a no-op in this mode and would leave
    the worker stuck at the reduced prefetch after one reconnect.

    The fix skips the reduction entirely when per-consumer QoS is in
    effect so the new consumer created on reconnect starts at the full
    prefetch count.
    """

    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.worker_prefetch_multiplier = 1
        app.conf.worker_concurrency = 4
        app.conf.task_acks_late = True
        # Default value, set explicitly to document intent: the fix only
        # applies when reduction is enabled. We must NOT silently rely on
        # the user-discovered workaround of disabling it.
        app.conf.worker_enable_prefetch_count_reduction = True
        return app

    # No RedisTestBroker xfail guard is needed: the ``quorum_queues/``
    # conftest forces a RabbitMQ broker, so this test class only ever
    # runs against amqp where the bug actually manifests.

    def test_skip_log_emitted_after_broker_restart(self, celery_setup: CeleryTestSetup):
        """The new ``Skipping prefetch count reduction`` log appears,
        and the legacy ``Temporarily reducing the prefetch count`` log
        does not."""
        queue = celery_setup.worker.worker_queue
        # Occupy all 4 pool slots with long-running tasks so there are
        # in-flight requests at the moment of broker disconnection.
        sig = group(long_running_task.s(420) for _ in range(4))
        sig.apply_async(queue=queue)
        # Wait until the tasks have actually been picked up by the pool
        # so ``active_requests`` is non-empty when the broker dies. Without
        # this barrier the test could pass on a pre-fix build because the
        # reduction code path would never even be entered.
        celery_setup.worker.wait_for_log("Starting long running task")

        celery_setup.broker.restart()

        celery_setup.worker.assert_log_exists(
            "Skipping prefetch count reduction after connection restart"
        )
        # The legacy reduction log must not appear: in per-consumer QoS
        # mode the reduction would silently strand the consumer at the
        # reduced prefetch forever. ``assert_log_does_not_exist`` polls
        # for a short window so this is not a stale-snapshot race.
        celery_setup.worker.assert_log_does_not_exist(
            "Temporarily reducing the prefetch count"
        )

    def test_worker_resumes_consuming_after_broker_restart(self, celery_setup: CeleryTestSetup):
        """A task submitted after broker restart still gets processed.

        Two long-running tasks are placed in flight so the connection
        error path actually enters the reduction code under test (without
        active requests the bug branch is never reached and the test
        would silently pass on a pre-fix build).

        Two in-flight is deliberately less than ``worker_concurrency=4``
        so that two pool slots remain free for the post-restart noop.
        After the broker restarts and ``task_acks_late`` causes the two
        long-running tasks to be redelivered:

        - On a pre-fix build the consumer is stranded at the reduced
          prefetch (max(1, 4-2)=2). Both prefetch slots are taken by the
          redelivered long-running tasks, so the noop is never fetched
          and the test times out.
        - On a post-fix build the consumer keeps its full prefetch of 4.
          The two redelivered long-running tasks take two prefetch slots
          and two pool slots; the noop is fetched into a free prefetch
          slot, dispatched to a free pool process, and completes.
        """
        queue = celery_setup.worker.worker_queue
        # Confirm the worker is healthy before perturbing it.
        assert noop.s().apply_async(queue=queue).get(timeout=RESULT_TIMEOUT) is None

        sig = group(long_running_task.s(420) for _ in range(2))
        sig.apply_async(queue=queue)
        celery_setup.worker.wait_for_log("Starting long running task")

        celery_setup.broker.restart()

        # After reconnect, a new task must still flow end-to-end.
        assert noop.s().apply_async(queue=queue).get(timeout=RESULT_TIMEOUT) is None
