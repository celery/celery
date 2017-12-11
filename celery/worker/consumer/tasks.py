"""Worker Task Consumer Bootstep."""
<<<<<<< HEAD
from __future__ import absolute_import, unicode_literals

=======
from typing import Mapping
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from kombu.common import QoS, ignore_errors

from celery import bootsteps
from celery.types import WorkerConsumerT
from celery.utils.log import get_logger

from .mingle import Mingle

__all__ = ('Tasks',)

logger = get_logger(__name__)
debug = logger.debug


class Tasks(bootsteps.StartStopStep):
    """Bootstep starting the task message consumer."""

    requires = (Mingle,)

    def __init__(self, c: WorkerConsumerT, **kwargs) -> None:
        c.task_consumer = c.qos = None
        super(Tasks, self).__init__(c, **kwargs)

    async def start(self, c: WorkerConsumerT) -> None:
        """Start task consumer."""
        c.update_strategies()

        # - RabbitMQ 3.3 completely redefines how basic_qos works..
        # This will detect if the new qos smenatics is in effect,
        # and if so make sure the 'apply_global' flag is set on qos updates.
        qos_global = not c.connection.qos_semantics_matches_spec

        # set initial prefetch count
        c.connection.default_channel.basic_qos(
            0, c.initial_prefetch_count, qos_global,
        )

        c.task_consumer = c.app.amqp.TaskConsumer(
            c.connection, on_decode_error=c.on_decode_error,
        )

        async def set_prefetch_count(prefetch_count: int) -> None:
            await c.task_consumer.qos(
                prefetch_count=prefetch_count,
                apply_global=qos_global,
            )
        c.qos = QoS(set_prefetch_count, c.initial_prefetch_count)

    async def stop(self, c: WorkerConsumerT) -> None:
        """Stop task consumer."""
        if c.task_consumer:
            debug('Canceling task consumer...')
            ignore_errors(c, c.task_consumer.cancel)

    async def shutdown(self, c: WorkerConsumerT) -> None:
        """Shutdown task consumer."""
        if c.task_consumer:
            self.stop(c)
            debug('Closing consumer channel...')
            ignore_errors(c, c.task_consumer.close)
            c.task_consumer = None

    def info(self, c: WorkerConsumerT) -> Mapping:
        """Return task consumer info."""
        return {'prefetch_count': c.qos.value if c.qos else 'N/A'}
