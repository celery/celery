"""Integration tests for broker-connection recovery (GH-9705).

Exercises the two fixes that prevent the worker from hanging indefinitely
after a broker connection loss:

1. ``Connection.stop()`` now correctly closes ``c.connection`` during
   ``blueprint.restart()``.  Before the fix, the inherited
   ``StartStopStep.stop()`` was a no-op (it guards on ``self.obj``, which
   is always ``None`` for the Connection bootstep), so the broken socket
   was silently leaked across every reconnect cycle.

2. ``on_connection_error_after_connected()`` now passes an explicit
   ``socket_timeout`` to ``collect()`` so that cleanup I/O on a dead
   connection cannot block forever.
"""
from unittest.mock import patch

import pytest

from celery.contrib.testing.worker import start_worker
from celery.worker.consumer.connection import Connection
from celery.worker.consumer.consumer import COLLECT_SOCKET_TIMEOUT

from .conftest import flaky
from .tasks import add

TIMEOUT = 30


@pytest.fixture()
def solo_worker(celery_session_app):
    """Dedicated solo-pool worker for connection-recovery integration tests."""
    with start_worker(
        celery_session_app,
        pool='solo',
        concurrency=1,
        perform_ping_check=False,
        shutdown_timeout=TIMEOUT,
    ) as worker:
        yield worker


class test_connection_recovery:
    """Integration tests for GH-9705: worker hang after broker connection loss."""

    def test_connection_stop_clears_connection(self, celery_session_app):
        """Connection.stop() must close c.connection and set it to None.

        Before the fix, StartStopStep.stop() was a no-op because it guards on
        self.obj, which is always None for the Connection bootstep.  The broken
        socket was silently leaked across every reconnect cycle.
        """
        # Use a real broker connection so we exercise the actual close path,
        # not just mock behaviour.
        conn = celery_session_app.connection()
        conn.connect()

        consumer = type('Consumer', (), {'connection': conn})()
        step = Connection.__new__(Connection)
        step.stop(consumer)

        assert consumer.connection is None

    @flaky
    def test_on_connection_error_after_connected_passes_collect_timeout(
        self, solo_worker
    ):
        """on_connection_error_after_connected() must call collect(socket_timeout=...).

        Without the fix, ``collect(socket_timeout=None)`` sets the *global*
        socket timeout to blocking-forever via ``socket.setdefaulttimeout()``.
        On a dead connection (e.g. TCP half-open after VM suspend/resume),
        this caused the worker to hang in ``_brpop_read()`` and never reach
        the reconnect step.
        """
        consumer = solo_worker.consumer

        with patch.object(consumer.connection, 'collect') as mock_collect:
            consumer.on_connection_error_after_connected(
                ConnectionError('simulated broker reset (GH-9705 regression)')
            )

        mock_collect.assert_called_once_with(socket_timeout=COLLECT_SOCKET_TIMEOUT)

    @flaky
    def test_tasks_complete_after_simulated_connection_error(self, solo_worker):
        """Worker remains functional after on_connection_error_after_connected().

        Verify that running on_connection_error_after_connected() with a
        bounded collect() timeout does not leave the worker in a broken state.
        """
        consumer = solo_worker.consumer

        # Simulate the connection-error cleanup path.
        with patch.object(consumer.connection, 'collect'):
            consumer.on_connection_error_after_connected(
                ConnectionError('simulated broker reset (GH-9705 regression)')
            )

        # Worker must still be able to process tasks.
        assert add.delay(2, 3).get(timeout=TIMEOUT) == 5
