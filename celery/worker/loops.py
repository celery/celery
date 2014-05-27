"""
celery.worker.loop
~~~~~~~~~~~~~~~~~~

The consumers highly-optimized inner loop.

"""
from __future__ import absolute_import

import socket

from celery.bootsteps import RUN
from celery.exceptions import WorkerShutdown, WorkerTerminate, WorkerLostError
from celery.utils.log import get_logger

from . import state

__all__ = ['asynloop', 'synloop']

logger = get_logger(__name__)
error = logger.error


def asynloop(obj, connection, consumer, blueprint, hub, qos, clock, RUN=RUN):
    """Non-blocking event loop consuming messages until connection is lost,
    or shutdown is requested."""
    update_qos = qos.update
    on_task_received = obj.create_task_handler()
    consumer.on_message = on_task_received
    consumer.consume()

    obj.on_ready()
    obj.controller.register_with_event_loop(hub)
    obj.register_with_event_loop(hub)

    # did_start_ok will verify that pool processes were able to start,
    # but this will only work the first time we start, as
    # maxtasksperchild will mess up metrics.
    if not obj.restart_count and not obj.pool.did_start_ok():
        raise WorkerLostError('Could not start worker processes')

    # FIXME: Use loop.run_forever
    # Tried and works, but no time to test properly before release.
    hub.propagate_errors = connection.connection_errors
    loop = hub.create_loop()

    try:
        while blueprint.state == RUN and obj.connection:
            # shutdown if signal handlers told us to.
            should_stop, should_terminate = (
                state.should_stop, state.should_terminate,
            )
            # False == EX_OK, so must use is not False
            if should_stop is not None and should_stop is not False:
                raise WorkerShutdown(should_stop)
            elif should_terminate is not None and should_stop is not False:
                raise WorkerTerminate(should_terminate)

            # We only update QoS when there is no more messages to read.
            # This groups together qos calls, and makes sure that remote
            # control commands will be prioritized over task messages.
            if qos.prev != qos.value:
                update_qos()

            try:
                next(loop)
            except StopIteration:
                loop = hub.create_loop()
    finally:
        try:
            hub.reset()
        except Exception as exc:
            error(
                'Error cleaning up after event loop: %r', exc, exc_info=1,
            )


def synloop(obj, connection, consumer, blueprint, hub, qos, clock, **kwargs):
    """Fallback blocking event loop for transports that doesn't support AIO."""
    update_qos = qos.update
    on_task_received = obj.create_task_handler()
    consumer.on_message = on_task_received
    consumer.consume()

    obj.on_ready()

    while blueprint.state == RUN and obj.connection:
        state.maybe_shutdown()
        if qos.prev != qos.value:
            update_qos()
        try:
            connection.drain_events(timeout=2.0)
        except socket.timeout:
            pass
        except socket.error:
            if blueprint.state == RUN:
                raise
