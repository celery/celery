import socket
from datetime import datetime

from dateutil.parser import parse as parse_iso8601
from carrot.connection import DjangoBrokerConnection, AMQPConnectionException

from celery import conf
from celery.utils import retry_over_time
from celery.worker.job import TaskWrapper
from celery.worker.revoke import revoked
from celery.worker.heartbeat import Heart
from celery.events import EventDispatcher
from celery.messaging import get_consumer_set, BroadcastConsumer
from celery.exceptions import NotRegistered
from celery.datastructures import SharedCounter


class CarrotListener(object):
    """Listen for messages received from the AMQP broker and
    move them the the bucket queue for task processing.

    :param ready_queue: See :attr:`ready_queue`.
    :param eta_scheduler: See :attr:`eta_scheduler`.

    .. attribute:: ready_queue

        The queue that holds tasks ready for processing immediately.

    .. attribute:: eta_scheduler

        Scheduler for paused tasks. Reasons for being paused include
        a countdown/eta or that it's waiting for retry.

    .. attribute:: logger

        The logger used.

    """

    def __init__(self, ready_queue, eta_scheduler, logger,
            initial_prefetch_count=2):
        self.amqp_connection = None
        self.task_consumer = None
        self.ready_queue = ready_queue
        self.eta_scheduler = eta_scheduler
        self.logger = logger
        self.prefetch_count = SharedCounter(initial_prefetch_count)
        self.event_dispatcher = None
        self.event_connection = None
        self.heart = None

    def start(self):
        """Start the consumer.

        If the connection is lost, it tries to re-establish the connection
        over time and restart consuming messages.

        """

        while True:
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
        it = task_consumer.iterconsume(limit=None)

        self.logger.debug("CarrotListener: Ready to accept tasks!")

        prev_pcount = None
        while True:
            if not prev_pcount or int(self.prefetch_count) != prev_pcount:
                self.task_consumer.qos(prefetch_count=int(self.prefetch_count))
                prev_pcount = int(self.prefetch_count)
            it.next()

    def stop(self):
        """Stop processing AMQP messages and close the connection
        to the broker."""
        self.close_connection()

    def handle_control_command(self, command):
        if command["command"] == "revoke":
            revoke_uuid = command["task_id"]
            revoked.add(revoke_uuid)
            self.logger.warn("Task %s marked as revoked." % revoke_uuid)
            return

    def receive_message(self, message_data, message):
        """The callback called when a new message is received.

        If the message has an ``eta`` we move it to the hold queue,
        otherwise we move it the bucket queue for immediate processing.

        """

        control = message_data.get("control")
        if control:
            print("RECV CONTROL: %s" % control)
            self.handle_control_command(control)
            return

        try:
            task = TaskWrapper.from_message(message, message_data,
                                            logger=self.logger,
                                            eventer=self.event_dispatcher)
        except NotRegistered, exc:
            self.logger.error("Unknown task ignored: %s" % (exc))
            return

        if task.task_id in revoked:
            self.logger.warn("Got revoked task from broker: %s[%s]" % (
                task.task_name, task.task_id))
            return

        eta = message_data.get("eta")

        self.event_dispatcher.send("task-received", uuid=task.task_id,
                                                    name=task.task_name,
                                                    args=task.args,
                                                    kwargs=task.kwargs,
                                                    retries=task.retries,
                                                    eta=eta)
        if eta:
            if not isinstance(eta, datetime):
                eta = parse_iso8601(eta)
            self.prefetch_count.increment()
            self.logger.info("Got task from broker: %s[%s] eta:[%s]" % (
                    task.task_name, task.task_id, eta))
            self.eta_scheduler.enter(task,
                                     eta=eta,
                                     callback=self.prefetch_count.decrement)
        else:
            self.logger.info("Got task from broker: %s[%s]" % (
                    task.task_name, task.task_id))
            self.ready_queue.put(task)

    def close_connection(self):
        """Close the AMQP connection."""
        if self.heart:
            self.heart.stop()
        if self.task_consumer:
            self.task_consumer.close()
            self.task_consumer = None
        if self.amqp_connection:
            self.logger.debug(
                    "CarrotListener: Closing connection to the broker...")
            self.amqp_connection.close()
            self.amqp_connection = None
        if self.event_connection:
            self.event_connection.close()
            self.event_connection = None
        self.event_dispatcher = None

    def reset_connection(self):
        """Reset the AMQP connection, and reinitialize the
        :class:`carrot.messaging.ConsumerSet` instance.

        Resets the task consumer in :attr:`task_consumer`.

        """
        self.logger.debug(
                "CarrotListener: Re-establishing connection to the broker...")
        self.close_connection()
        self.amqp_connection = self._open_connection()
        self.logger.debug("CarrotListener: Connection Established.")
        self.task_consumer = get_consumer_set(connection=self.amqp_connection)
        self.broadcast_consumer = BroadcastConsumer(self.amqp_connection)
        self.task_consumer.add_consumer(self.broadcast_consumer)
        self.task_consumer.register_callback(self.receive_message)
        self.event_dispatcher = EventDispatcher(self.amqp_connection)
        self.heart = Heart(self.event_dispatcher)
        #self.heart.start()

    def _open_connection(self):
        """Retries connecting to the AMQP broker over time.

        See :func:`celery.utils.retry_over_time`.

        """

        def _connection_error_handler(exc, interval):
            """Callback handler for connection errors."""
            self.logger.error("AMQP Listener: Connection Error: %s. " % exc
                     + "Trying again in %d seconds..." % interval)

        def _establish_connection():
            """Establish a connection to the AMQP broker."""
            conn = DjangoBrokerConnection()
            connected = conn.connection # Connection is established lazily.
            return conn

        if not conf.AMQP_CONNECTION_RETRY:
            return _establish_connection()

        conn = retry_over_time(_establish_connection, (socket.error, IOError),
                               errback=_connection_error_handler,
                               max_retries=conf.AMQP_CONNECTION_MAX_RETRIES)
        return conn


