from __future__ import generators

import socket
import warnings
from datetime import datetime

from dateutil.parser import parse as parse_iso8601
from carrot.connection import AMQPConnectionException

from celery import conf
from celery.utils import noop, retry_over_time
from celery.worker.job import TaskWrapper, InvalidTaskError
from celery.worker.revoke import revoked
from celery.worker.control import ControlDispatch
from celery.worker.heartbeat import Heart
from celery.events import EventDispatcher
from celery.messaging import establish_connection
from celery.messaging import get_consumer_set, BroadcastConsumer
from celery.exceptions import NotRegistered
from celery.datastructures import SharedCounter

RUN = 0x0
CLOSE = 0x1


class CarrotListener(object):
    """Listen for messages received from the broker and
    move them the the ready queue for task processing.

    :param ready_queue: See :attr:`ready_queue`.
    :param eta_schedule: See :attr:`eta_schedule`.

    .. attribute:: ready_queue

        The queue that holds tasks ready for processing immediately.

    .. attribute:: eta_schedule

        Scheduler for paused tasks. Reasons for being paused include
        a countdown/eta or that it's waiting for retry.

    .. attribute:: logger

        The logger used.

    """

    def __init__(self, ready_queue, eta_schedule, logger,
            init_callback=noop, send_events=False, hostname=None,
            initial_prefetch_count=2):
        self.connection = None
        self.task_consumer = None
        self.ready_queue = ready_queue
        self.eta_schedule = eta_schedule
        self.send_events = send_events
        self.init_callback = init_callback
        self.logger = logger
        self.hostname = hostname or socket.gethostname()
        self.control_dispatch = ControlDispatch(logger=logger,
                                                hostname=self.hostname,
                                                listener=self)
        self.prefetch_count = SharedCounter(initial_prefetch_count)
        self.event_dispatcher = None
        self.heart = None
        self._state = None

    def start(self):
        """Start the consumer.

        If the connection is lost, it tries to re-establish the connection
        and restarts consuming messages.

        """

        self.init_callback(self)

        while 1:
            self.reset_connection()
            try:
                self.consume_messages()
            except (socket.error, AMQPConnectionException, IOError):
                self.logger.error("CarrotListener: Connection to broker lost."
                                + " Trying to re-establish connection...")

    def consume_messages(self):
        """Consume messages forever (or until an exception is raised)."""
        task_consumer = self.task_consumer

        self.logger.debug("CarrotListener: Starting message consumer...")
        wait_for_message = self._detect_wait_method()(limit=None).next
        self.logger.debug("CarrotListener: Ready to accept tasks!")

        prev_pcount = None
        while 1:
            pcount = int(self.prefetch_count) # SharedCounter() -> int()
            if not prev_pcount or pcount != prev_pcount:
                task_consumer.qos(prefetch_count=pcount)
                prev_pcount = pcount

            wait_for_message()

    def on_task(self, task, eta=None):
        """Handle received task.

        If the task has an ``eta`` we enter it into the ETA schedule,
        otherwise we move it the ready queue for immediate processing.

        """

        if task.task_id in revoked:
            self.logger.warn("Got revoked task from broker: %s[%s]" % (
                task.task_name, task.task_id))
            return task.on_ack()

        self.event_dispatcher.send("task-received", uuid=task.task_id,
                name=task.task_name, args=task.args, kwargs=task.kwargs,
                retries=task.retries, eta=eta)

        if eta:
            if not isinstance(eta, datetime):
                eta = parse_iso8601(eta)
            self.prefetch_count.increment()
            self.logger.info("Got task from broker: %s[%s] eta:[%s]" % (
                    task.task_name, task.task_id, eta))
            self.eta_schedule.enter(task, eta=eta,
                                    callback=self.prefetch_count.decrement)
        else:
            self.logger.info("Got task from broker: %s[%s]" % (
                    task.task_name, task.task_id))
            self.ready_queue.put(task)

    def receive_message(self, message_data, message):
        """The callback called when a new message is received. """

        # Handle task
        if message_data.get("task"):
            try:
                task = TaskWrapper.from_message(message, message_data,
                                                logger=self.logger,
                                                eventer=self.event_dispatcher)
            except NotRegistered, exc:
                self.logger.error("Unknown task ignored: %s: %s" % (
                        str(exc), message_data))
                message.ack()
            except InvalidTaskError, exc:
                self.logger.error("Invalid task ignored: %s: %s" % (
                        str(exc), message_data))
                message.ack()
            else:
                self.on_task(task, eta=message_data.get("eta"))
            return

        # Handle control command
        control = message_data.get("control")
        if control:
            self.control_dispatch.dispatch_from_message(control)
            return

        warnings.warn(RuntimeWarning(
            "Received and deleted unknown message. Wrong destination?!? \
             the message was: %s" % message_data))
        message.ack()

    def close_connection(self):
        self.logger.debug("CarrotListener: "
                          "Closing connection to broker...")
        self.connection = self.connection and self.connection.close()

    def stop_consumers(self, close=True):
        if not self._state == RUN:
            return
        self._state = CLOSE

        if self.heart:
            self.logger.debug("Heart: Going into cardiac arrest...")
            self.heart = self.heart.stop()

        self.logger.debug("TaskConsumer: Shutting down...")
        self.task_consumer = self.task_consumer and self.task_consumer.close()

        if self.event_dispatcher:
            self.logger.debug("EventDispatcher: Shutting down...")
            self.event_dispatcher = self.event_dispatcher.close()

        if close:
            self.close_connection()

    def on_decode_error(self, message, exc):
        """Callback called if the message had decoding errors.

        :param message: The message with errors.
        :param exc: The original exception instance.

        """
        self.logger.critical("Message decoding error: %s "
                             "(type:%s encoding:%s raw:'%s')" % (
                                exc, message.content_type,
                                message.content_encoding, message.body))
        message.ack()

    def reset_connection(self):
        self.logger.debug(
                "CarrotListener: Re-establishing connection to the broker...")
        self.stop_consumers()

        try:
            # TaskBucket supports clear directly.
            self.ready_queue.clear()
        except AttributeError:
            # Use the underlying deque of regular Queue
            self.ready_queue.queue.clear()
        self.eta_schedule.clear()

        self.connection = self._open_connection()
        self.logger.debug("CarrotListener: Connection Established.")
        self.task_consumer = get_consumer_set(connection=self.connection)
        self.task_consumer.on_decode_error = self.on_decode_error
        self.broadcast_consumer = BroadcastConsumer(self.connection,
                                                    hostname=self.hostname)
        self.task_consumer.register_callback(self.receive_message)
        self.event_dispatcher = EventDispatcher(self.connection,
                                                enabled=self.send_events)
        self.heart = Heart(self.event_dispatcher)
        self.heart.start()

        self._state = RUN

    def _mainloop(self, **kwargs):
        while 1:
            yield self.connection.connection.drain_events()

    def _detect_wait_method(self):
        if hasattr(self.connection.connection, "drain_events"):
            self.broadcast_consumer.register_callback(self.receive_message)
            self.task_consumer.iterconsume()
            self.broadcast_consumer.iterconsume()
            return self._mainloop
        else:
            self.task_consumer.add_consumer(self.broadcast_consumer)
            return self.task_consumer.iterconsume

    def _open_connection(self):
        """Retries connecting to the AMQP broker over time.

        See :func:`celery.utils.retry_over_time`.

        """

        def _connection_error_handler(exc, interval):
            """Callback handler for connection errors."""
            self.logger.error("CarrotListener: Connection Error: %s. " % exc
                     + "Trying again in %d seconds..." % interval)

        def _establish_connection():
            """Establish a connection to the broker."""
            conn = establish_connection()
            conn.connect() # Connection is established lazily, so connect.
            return conn

        if not conf.BROKER_CONNECTION_RETRY:
            return _establish_connection()

        conn = retry_over_time(_establish_connection, (socket.error, IOError),
                               errback=_connection_error_handler,
                               max_retries=conf.BROKER_CONNECTION_MAX_RETRIES)
        return conn

    def stop(self):
        self.logger.debug("CarrotListener: Stopping consumers...")
        self.stop_consumers(close=False)
