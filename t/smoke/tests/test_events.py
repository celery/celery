"""Remote control of the events of a worker started with ``-E`` (#10941).

A worker started with ``-E`` sends the events of every group. Disabling its
events with the ``disable_events`` remote control command has to stop the task
events, while the events of the ``worker`` group, the heartbeats, keep coming.
``enable_events`` has to bring the task events back.
"""
from __future__ import annotations

import socket
from time import monotonic

import pytest
from pytest_celery import RESULT_TIMEOUT, CeleryTestSetup

from t.integration.tasks import add

# Longer than the default worker heartbeat interval of 2 seconds, so that the
# silence of the task events is observed next to events that do arrive.
QUIET_PERIOD = 6


def drain_events(connection, until=None, timeout: float = RESULT_TIMEOUT) -> bool:
    """Read events until ``until()`` is true, or for ``timeout`` seconds."""
    deadline = monotonic() + timeout
    while not (until and until()):
        remaining = deadline - monotonic()
        if remaining <= 0:
            return False
        try:
            connection.drain_events(timeout=min(remaining, 1))
        except socket.timeout:
            pass
    return True


class test_worker_events:
    @pytest.fixture
    def default_worker_command(self, default_worker_container_cls):
        return default_worker_container_cls.command("-E")

    def test_disable_and_enable_events_on_worker_started_with_E(self, celery_setup: CeleryTestSetup):
        app = celery_setup.app
        worker = celery_setup.worker
        hostname = worker.hostname()
        events = []

        def from_worker(*types: str) -> list[dict]:
            return [
                event for event in events
                if event["hostname"] == hostname and event["type"].startswith(types)
            ]

        def run_task(x: int, y: int) -> str:
            result = add.s(x, y).apply_async(queue=worker.worker_queue)
            assert result.get(timeout=RESULT_TIMEOUT) == x + y
            return result.id

        def control(command: str) -> dict:
            replies = getattr(app.control, command)(
                destination=[hostname], reply=True, timeout=RESULT_TIMEOUT,
            )
            assert len(replies) == 1
            return replies[0][hostname]

        with app.connection_for_read() as connection:
            receiver = app.events.Receiver(
                connection, handlers={"*": events.append},
            )
            with receiver.consumer_context(wakeup=False) as (conn, _, _):
                # The subscription to the events only starts with the first
                # read: wait for a heartbeat to know that events are received.
                assert drain_events(conn, lambda: from_worker("worker-heartbeat"))

                # The worker sends the events of every group.
                task_id = run_task(1, 2)
                assert drain_events(
                    conn,
                    lambda: any(e["uuid"] == task_id for e in from_worker("task-succeeded")),
                )

                # The task events stop, the heartbeats do not.
                assert control("disable_events") == {"ok": "task events disabled"}
                events.clear()
                run_task(3, 4)
                drain_events(conn, timeout=QUIET_PERIOD)
                assert from_worker("worker-heartbeat")
                assert not from_worker("task-")

                # Disabling twice is a no-op.
                assert control("disable_events") == {"ok": "task events already disabled"}

                # The task events are back.
                assert control("enable_events") == {"ok": "task events enabled"}
                events.clear()
                task_id = run_task(5, 6)
                assert drain_events(
                    conn,
                    lambda: any(e["uuid"] == task_id for e in from_worker("task-succeeded")),
                )
