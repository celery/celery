from unittest.mock import Mock, patch

import pytest

from celery import bootsteps
from celery.worker.loops import synloop


def test_synloop_perform_pending_operations_on_system_exit():
    # Mock dependencies
    obj = Mock()
    connection = Mock()
    consumer = Mock()
    blueprint = Mock()
    hub = Mock()
    qos = Mock()
    heartbeat = Mock()
    clock = Mock()

    # Set up the necessary attributes
    obj.create_task_handler.return_value = Mock()
    obj.perform_pending_operations = Mock()
    obj.on_ready = Mock()
    obj.pool.is_green = False
    obj.connection = True

    blueprint.state = bootsteps.RUN  # Simulate RUN state

    qos.prev = qos.value = Mock()

    # Mock state.maybe_shutdown to raise SystemExit
    with patch("celery.worker.loops.state") as mock_state:
        mock_state.maybe_shutdown.side_effect = SystemExit

        # Call synloop and expect SystemExit to be raised
        with pytest.raises(SystemExit):
            synloop(
                obj,
                connection,
                consumer,
                blueprint,
                hub,
                qos,
                heartbeat,
                clock,
                hbrate=2.0,
            )

    # Assert that perform_pending_operations was called even after SystemExit
    obj.perform_pending_operations.assert_called_once()

    # Assert that connection.drain_events was called
    connection.drain_events.assert_called_with(timeout=2.0)

    # Assert other important method calls
    obj.on_ready.assert_called_once()
    consumer.consume.assert_called_once()
