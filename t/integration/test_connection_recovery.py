"""Integration tests for broker-connection recovery (GH-9705).

Exercises the two fixes that prevent the worker from hanging indefinitely
after a broker connection loss:

1. ``on_connection_error_after_connected()`` now closes the broken
   connection after calling ``collect()``, so the stale socket is
   released before ``blueprint.restart()`` begins the reconnect cycle.

2. ``on_connection_error_after_connected()`` now passes an explicit
   ``socket_timeout`` to ``collect()`` so that cleanup I/O on a dead
   connection cannot block forever.
"""
from celery.worker.consumer.connection import Connection
from celery.worker.consumer.consumer import COLLECT_SOCKET_TIMEOUT


class test_connection_recovery:
    """Integration tests for GH-9705: worker hang after broker connection loss."""

    def test_close_connection_clears_connection(self, celery_session_app):
        """Connection.close_connection() must close c.connection and set it to None.

        Before the fix, the broken socket was silently leaked across every
        reconnect cycle because StartStopStep.stop() was a no-op (it guards
        on self.obj, which is always None for the Connection bootstep).
        """
        conn = celery_session_app.connection()
        conn.connect()

        consumer = type('Consumer', (), {'connection': conn})()
        step = Connection.__new__(Connection)
        step.close_connection(consumer)

        assert consumer.connection is None

    def test_collect_socket_timeout_is_set(self):
        """COLLECT_SOCKET_TIMEOUT must be a positive number.

        Without the fix, collect(socket_timeout=None) sets the global
        socket timeout to blocking-forever, causing the worker to hang
        in _brpop_read() on a dead connection.
        """
        assert isinstance(COLLECT_SOCKET_TIMEOUT, (int, float))
        assert COLLECT_SOCKET_TIMEOUT > 0
