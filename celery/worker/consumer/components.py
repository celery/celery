from __future__ import absolute_import

import socket
import threading

from kombu.common import QoS

from celery.datastructures import AttributeDict
from celery.utils.log import get_logger

from celery.worker.bootsteps import StartStopComponent
from celery.worker.control import Panel
from celery.worker.heartbeat import Heart

logger = get_logger(__name__)
info, error, debug = logger.info, logger.error, logger.debug


class ConsumerConnection(StartStopComponent):
    name = 'consumer.connection'
    delay_shutdown = True

    def __init__(self, c, **kwargs):
        c.connection = None

    def start(self, c):
        debug('Re-establishing connection to the broker...')
        c.connection = c._open_connection()
        # Re-establish the broker connection and setup the task consumer.
        info('consumer: Connected to %s.', c.connection.as_uri())

    def stop(self, c):
        pass

    def shutdown(self, c):
        # We must set self.connection to None here, so
        # that the green pidbox thread exits.
        connection, c.connection = c.connection, None

        if connection:
            c.maybe_conn_error(connection.close)


class Events(StartStopComponent):
    name = 'consumer.events'
    requires = (ConsumerConnection, )

    def __init__(self, c, send_events=None, **kwargs):
        self.app = c.app
        c.event_dispatcher = None
        self.send_events = send_events

    def start(self, c):
        # Flush events sent while connection was down.
        prev_event_dispatcher = c.event_dispatcher
        c.event_dispatcher = self.app.events.Dispatcher(c.connection,
                                                hostname=c.hostname,
                                                enabled=self.send_events)
        if prev_event_dispatcher:
            c.event_dispatcher.copy_buffer(prev_event_dispatcher)
            c.event_dispatcher.flush()

    def stop(self, c):
        if c.event_dispatcher:
            debug('Shutting down event dispatcher...')
            c.event_dispatcher = \
                    c.maybe_conn_error(c.event_dispatcher.close)

    def shutdown(self, c):
        self.stop(c)


class Heartbeat(StartStopComponent):
    name = 'consumer.heart'
    requires = (Events, )

    def __init__(self, c, **kwargs):
        c.heart = None

    def start(self, c):
        c.heart = Heart(c.timer, c.event_dispatcher)
        c.heart.start()

    def stop(self, c):
        if c.heart:
            # Stop the heartbeat thread if it's running.
            debug('Heart: Going into cardiac arrest...')
            c.heart = c.heart.stop()

    def shutdown(self, c):
        self.stop(c)


class Controller(StartStopComponent):
    name = 'consumer.controller'
    requires = (Events, )

    _pidbox_node_shutdown = None
    _pidbox_node_stopped = None

    def __init__(self, c, **kwargs):
        self.app = c.app
        pidbox_state = AttributeDict(
            app=c.app, hostname=c.hostname, consumer=c,
        )
        self.pidbox_node = self.app.control.mailbox.Node(
            c.hostname, state=pidbox_state, handlers=Panel.data,
        )
        self.broadcast_consumer = None
        self.consumer = c

    def start(self, c):
        self.reset_pidbox_node()

    def stop(self, c):
        pass

    def shutdown(self, c):
        self.stop_pidbox_node()
        if self.broadcast_consumer:
            debug('Cancelling broadcast consumer...')
            c.maybe_conn_error(self.broadcast_consumer.cancel)
            self.broadcast_consumer = None

    def on_control(self, body, message):
        """Process remote control command message."""
        try:
            self.pidbox_node.handle_message(body, message)
        except KeyError as exc:
            error('No such control command: %s', exc)
        except Exception as exc:
            error('Control command error: %r', exc, exc_info=True)
            self.reset_pidbox_node()

    def reset_pidbox_node(self):
        """Sets up the process mailbox."""
        c = self.consumer
        self.stop_pidbox_node()
        # close previously opened channel if any.
        if self.pidbox_node and self.pidbox_node.channel:
            c.maybe_conn_error(self.pidbox_node.channel.close)

        if c.pool is not None and c.pool.is_green:
            return c.pool.spawn_n(self._green_pidbox_node)
        self.pidbox_node.channel = c.connection.channel()
        self.broadcast_consumer = self.pidbox_node.listen(
                                        callback=self.on_control)

    def stop_pidbox_node(self):
        c = self.consumer
        if self._pidbox_node_stopped:
            self._pidbox_node_shutdown.set()
            debug('Waiting for broadcast thread to shutdown...')
            self._pidbox_node_stopped.wait()
            self._pidbox_node_stopped = self._pidbox_node_shutdown = None
        elif self.broadcast_consumer:
            debug('Closing broadcast channel...')
            self.broadcast_consumer = \
                c.maybe_conn_error(self.broadcast_consumer.channel.close)

    def _green_pidbox_node(self):
        """Sets up the process mailbox when running in a greenlet
        environment."""
        # THIS CODE IS TERRIBLE
        # Luckily work has already started rewriting the Consumer for 4.0.
        self._pidbox_node_shutdown = threading.Event()
        self._pidbox_node_stopped = threading.Event()
        c = self.consumer
        try:
            with c._open_connection() as conn:
                info('pidbox: Connected to %s.', conn.as_uri())
                self.pidbox_node.channel = conn.default_channel
                self.broadcast_consumer = self.pidbox_node.listen(
                                            callback=self.on_control)
                with self.broadcast_consumer:
                    while not self._pidbox_node_shutdown.isSet():
                        try:
                            conn.drain_events(timeout=1.0)
                        except socket.timeout:
                            pass
        finally:
            self._pidbox_node_stopped.set()


class Tasks(StartStopComponent):
    name = 'consumer.tasks'
    requires = (Controller, )
    last = True

    def __init__(self, c, initial_prefetch_count=2, **kwargs):
        self.app = c.app
        c.task_consumer = None
        c.qos = None
        self.initial_prefetch_count = initial_prefetch_count

    def start(self, c):
        c.task_consumer = self.app.amqp.TaskConsumer(c.connection,
                                    on_decode_error=c.on_decode_error)
        # QoS: Reset prefetch window.
        c.qos = QoS(c.task_consumer, self.initial_prefetch_count)
        c.qos.update()

    def stop(self, c):
        pass

    def shutdown(self, c):
        if c.task_consumer:
            debug('Cancelling task consumer...')
            c.maybe_conn_error(c.task_consumer.cancel)
            debug('Closing consumer channel...')
            c.maybe_conn_error(c.task_consumer.close)
            c.task_consumer = None
