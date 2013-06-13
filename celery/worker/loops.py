"""
celery.worker.loop
~~~~~~~~~~~~~~~~~~

The consumers highly-optimized inner loop.

"""
from __future__ import absolute_import

import socket

from time import sleep
from types import GeneratorType as generator

from kombu.utils.eventio import READ, WRITE, ERR

from celery.bootsteps import CLOSE
from celery.exceptions import SystemTerminate
from celery.five import Empty
from celery.utils.log import get_logger

from . import state

logger = get_logger(__name__)
error = logger.error


def asynloop(obj, connection, consumer, blueprint, hub, qos,
             heartbeat, clock, hbrate=2.0,
             sleep=sleep, min=min, Empty=Empty):
    """Non-blocking eventloop consuming messages until connection is lost,
    or shutdown is requested."""

    hub.init()
    update_qos = qos.update
    update_readers = hub.update_readers
    readers, writers = hub.readers, hub.writers
    poll = hub.poller.poll
    fire_timers = hub.fire_timers
    hub_add = hub.add
    hub_remove = hub.remove
    scheduled = hub.timer._queue
    hbtick = connection.heartbeat_check
    conn_poll_start = connection.transport.on_poll_start
    conn_poll_empty = connection.transport.on_poll_empty
    pool_poll_start = obj.pool.on_poll_start
    drain_nowait = connection.drain_nowait
    on_task_callbacks = hub.on_task
    keep_draining = connection.transport.nb_keep_draining
    errors = connection.connection_errors
    hub_add, hub_remove = hub.add, hub.remove

    on_task_received = obj.create_task_handler(on_task_callbacks)

    if heartbeat and connection.supports_heartbeats:
        hub.timer.apply_interval(
            heartbeat * 1000.0 / hbrate, hbtick, (hbrate, ))

    consumer.callbacks = [on_task_received]
    consumer.consume()
    obj.on_ready()

    try:
        while blueprint.state != CLOSE and obj.connection:
            # shutdown if signal handlers told us to.
            if state.should_stop:
                raise SystemExit()
            elif state.should_terminate:
                raise SystemTerminate()

            # fire any ready timers, this also returns
            # the number of seconds until we need to fire timers again.
            poll_timeout = fire_timers(propagate=errors) if scheduled else 1

            # We only update QoS when there is no more messages to read.
            # This groups together qos calls, and makes sure that remote
            # control commands will be prioritized over task messages.
            if qos.prev != qos.value:
                update_qos()

            #print('[[[HUB]]]: %s' % (hub.repr_active(), ))

            update_readers(conn_poll_start())
            pool_poll_start(hub)
            if readers or writers:
                connection.more_to_read = True
                while connection.more_to_read:
                    try:
                        events = poll(poll_timeout)
                        #print('[[[EV]]]: %s' % (hub.repr_events(events), ))
                    except ValueError:  # Issue 882
                        return

                    if not events:
                        conn_poll_empty()
                    for fileno, event in events or ():
                        cb = None
                        try:
                            if event & READ:
                                cb = readers[fileno]
                            elif event & WRITE:
                                cb = writers[fileno]
                            elif event & ERR:
                                cb = (readers.get(fileno) or
                                      writers.get(fileno))
                        except (KeyError, Empty):
                            continue
                        if cb is None:
                            continue
                        try:
                            if isinstance(cb, generator):
                                try:
                                    next(cb)
                                except StopIteration:
                                    hub_remove(fileno)
                                except Exception:
                                    hub_remove(fileno)
                                    raise
                            else:
                                try:
                                    cb(fileno, event)
                                except Empty:
                                    continue
                        except socket.error:
                            if blueprint.state != CLOSE:  # pragma: no cover
                                raise
                    if keep_draining:
                        drain_nowait()
                        poll_timeout = 0
                    else:
                        connection.more_to_read = False
            else:
                # no sockets yet, startup is probably not done.
                sleep(min(poll_timeout, 0.1))
    finally:
        try:
            hub.close()
        except Exception as exc:
            error(
                'Error cleaning up after eventloop: %r', exc, exc_info=1,
            )


def synloop(obj, connection, consumer, blueprint, hub, qos,
            heartbeat, clock, hbrate=2.0, **kwargs):
    """Fallback blocking eventloop for transports that doesn't support AIO."""

    on_task_received = obj.create_task_handler([])
    consumer.register_callback(on_task_received)
    consumer.consume()

    obj.on_ready()

    while blueprint.state != CLOSE and obj.connection:
        state.maybe_shutdown()
        if qos.prev != qos.value:
            qos.update()
        try:
            connection.drain_events(timeout=2.0)
        except socket.timeout:
            pass
        except socket.error:
            if blueprint.state != CLOSE:
                raise
