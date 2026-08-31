"""Smoke tests for quorum queue QoS behavior.

Verifies that tasks execute correctly on quorum queues with QoS settings
that differ from classic queues (no global QoS).

See https://github.com/celery/celery/issues/9929
"""
from __future__ import annotations

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup
from pytest_docker_tools.wrappers.container import wait_for_callable

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


def _wait_for_log_count(worker, log: str, count: int, timeout: int = RESULT_TIMEOUT) -> None:
    """Block until ``log`` appears at least ``count`` times in worker logs.

    ``pytest_celery``'s ``wait_for_log`` only checks substring presence,
    so calling it twice for the same string returns immediately after the
    first match. Tests that need N tasks to actually be in flight at the
    moment of a broker disconnect need a counted barrier to avoid a race
    where only one task has started when the disconnect fires.
    """
    wait_for_callable(
        message=f"Waiting for worker to log '{log}' at least {count} times",
        func=lambda: worker.logs().count(log) >= count,
        timeout=timeout,
    )


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
        # Wait until all 4 tasks have actually been picked up by the pool
        # so ``active_requests`` has the expected size when the broker
        # dies. A plain substring ``wait_for_log`` would return as soon as
        # the first task logs, which could let the test pass on a pre-fix
        # build where fewer active requests mean a smaller (or no)
        # reduction.
        _wait_for_log_count(celery_setup.worker, "Starting long running task", 4)

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


class test_quorum_qos_worker_resumes_after_reconnect:
    """Liveness regression test for the celery/celery#9512 reconnect path.

    Split from ``test_quorum_qos_prefetch_reduction_skipped_on_reconnect``
    because it requires
    ``worker_cancel_long_running_tasks_on_connection_loss`` to free pool
    slots after the restart. Enabling that setting drains
    ``active_requests`` on disconnect, which would mask the #9512
    regression in the skip-log test, so the two scenarios use separate
    worker apps.
    """

    @pytest.fixture
    def default_worker_app(self, default_worker_app: Celery) -> Celery:
        app = default_worker_app
        app.conf.worker_prefetch_multiplier = 1
        app.conf.worker_concurrency = 4
        app.conf.task_acks_late = True
        app.conf.worker_enable_prefetch_count_reduction = True
        # Terminate in-flight acks_late tasks when the connection drops so
        # their pool slots are released before the reconnect. Without this
        # the original long-running tasks keep occupying their pool slots
        # while the broker also redelivers them (acks_late); the original
        # and redelivered copies then fill every pool process and the
        # post-restart probe task below can never be dispatched.
        app.conf.worker_cancel_long_running_tasks_on_connection_loss = True
        return app

    # No RedisTestBroker xfail guard is needed: the ``quorum_queues/``
    # conftest forces a RabbitMQ broker, so this test class only ever
    # runs against amqp where the bug actually manifests.

    def test_worker_resumes_consuming_after_broker_restart(self, celery_setup: CeleryTestSetup):
        """A task submitted after a broker restart still gets processed.

        End-to-end liveness check for the reconnect path on quorum-queue
        per-consumer QoS. Two acks_late long-running tasks are put in
        flight so that, at the moment the broker dies, two pool processes
        are genuinely busy and the broker still owns the (unacked)
        messages.

        ``worker_cancel_long_running_tasks_on_connection_loss`` (set in
        the fixture) terminates those two in-flight tasks on disconnect,
        freeing their pool slots. After the restart the messages are
        redelivered (acks_late); on a post-#9512 build the consumer keeps
        its full prefetch of 4 (the reduction is skipped under
        per-consumer QoS), so the redelivered tasks and the trailing noop
        each find a free pool process and the noop completes within the
        timeout.

        Note: this test does not by itself distinguish the #9512 pre/post
        fix behaviour. Enabling cancellation removes the tasks from
        ``active_requests`` synchronously (``Request.cancel`` ->
        ``_announce_cancelled`` -> ``task_ready``), so the legacy
        reduction would read ``len(active_requests) == 0`` rather than 2.
        The discriminating #9512 assertion lives in
        ``test_skip_log_emitted_after_broker_restart``; here we only
        assert that work resumes flowing after the reconnect.
        """
        queue = celery_setup.worker.worker_queue
        # Confirm the worker is healthy before perturbing it.
        assert noop.s().apply_async(queue=queue).get(timeout=RESULT_TIMEOUT) is None

        sig = group(long_running_task.s(420) for _ in range(2))
        sig.apply_async(queue=queue)
        # Both tasks must actually be running (occupying two pool
        # processes) before the broker restart, otherwise there are no
        # busy slots to free and the scenario is not exercised. A single
        # ``wait_for_log`` would return as soon as the first task starts.
        _wait_for_log_count(celery_setup.worker, "Starting long running task", 2)

        celery_setup.broker.restart()

        # After reconnect, a new task must still flow end-to-end.
        assert noop.s().apply_async(queue=queue).get(timeout=RESULT_TIMEOUT) is None
