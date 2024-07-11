"""Worker Task Consumer Bootstep."""

from __future__ import annotations

import warnings

from kombu.common import QoS, ignore_errors

from celery import bootsteps
from celery.exceptions import CeleryWarning
from celery.utils.log import get_logger

from .mingle import Mingle

__all__ = ('Tasks',)

logger = get_logger(__name__)
debug = logger.debug


ETA_TASKS_NO_GLOBAL_QOS_WARNING = """
Detected quorum queue "%r", disabling global QoS.
With global QoS disabled, ETA tasks may not function as expected. Instead of adjusting
the prefetch count dynamically, ETA tasks will occupy the prefetch buffer, potentially
blocking other tasks from being consumed. To mitigate this, either set a high prefetch
count or avoid using quorum queues until the ETA mechanism is updated to support a
disabled global QoS, which is required for quorum queues.
"""


class Tasks(bootsteps.StartStopStep):
    """Bootstep starting the task message consumer."""

    requires = (Mingle,)

    def __init__(self, c, **kwargs):
        c.task_consumer = c.qos = None
        super().__init__(c, **kwargs)

    def start(self, c):
        """Start task consumer."""
        c.update_strategies()

        qos_global = self.qos_global(c)

        # set initial prefetch count
        c.connection.default_channel.basic_qos(
            0, c.initial_prefetch_count, qos_global,
        )

        c.task_consumer = c.app.amqp.TaskConsumer(
            c.connection, on_decode_error=c.on_decode_error,
        )

        def set_prefetch_count(prefetch_count):
            return c.task_consumer.qos(
                prefetch_count=prefetch_count,
                apply_global=qos_global,
            )
        c.qos = QoS(set_prefetch_count, c.initial_prefetch_count)

    def stop(self, c):
        """Stop task consumer."""
        if c.task_consumer:
            debug('Canceling task consumer...')
            ignore_errors(c, c.task_consumer.cancel)

    def shutdown(self, c):
        """Shutdown task consumer."""
        if c.task_consumer:
            self.stop(c)
            debug('Closing consumer channel...')
            ignore_errors(c, c.task_consumer.close)
            c.task_consumer = None

    def info(self, c):
        """Return task consumer info."""
        return {'prefetch_count': c.qos.value if c.qos else 'N/A'}

    def qos_global(self, c) -> bool:
        """Determine if global QoS should be applied.

        Additional information:
            https://www.rabbitmq.com/docs/consumer-prefetch
            https://www.rabbitmq.com/docs/quorum-queues#global-qos
        """
        # - RabbitMQ 3.3 completely redefines how basic_qos works...
        # This will detect if the new qos semantics is in effect,
        # and if so make sure the 'apply_global' flag is set on qos updates.
        qos_global = not c.connection.qos_semantics_matches_spec

        if c.app.conf.worker_detect_quorum_queues:
            using_quorum_queues, qname = self.detect_quorum_queues(c)
            if using_quorum_queues:
                qos_global = False
                # The ETA tasks mechanism requires additional work for Celery to fully support
                # quorum queues. Warn the user that ETA tasks may not function as expected until
                # this is done so we can at least support quorum queues partially for now.
                warnings.warn(ETA_TASKS_NO_GLOBAL_QOS_WARNING % (qname,), CeleryWarning)

        return qos_global

    def detect_quorum_queues(self, c) -> tuple[bool, str]:
        """Detect if any of the queues are quorum queues.

        Returns:
            tuple[bool, str]: A tuple containing a boolean indicating if any of the queues are quorum queues
            and the name of the first quorum queue found or an empty string if no quorum queues were found.
        """
        is_rabbitmq_broker = c.app.conf.broker_url.startswith(("amqp", "pyamqp"))

        if is_rabbitmq_broker:
            queues = c.app.amqp.queues
            for qname in queues:
                qarguments = queues[qname].queue_arguments or {}
                if qarguments.get("x-queue-type") == "quorum":
                    return True, qname

        return False, ""
