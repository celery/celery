"""Worker Task Consumer Bootstep."""
from kombu.common import QoS, ignore_errors

from celery import bootsteps
from celery.utils.log import get_logger

from .mingle import Mingle

__all__ = ('Tasks',)

logger = get_logger(__name__)
debug = logger.debug


class Tasks(bootsteps.StartStopStep):
    """Bootstep starting the task message consumer."""

    requires = (Mingle,)

    def __init__(self, c, **kwargs):
        c.qos = None
        c.task_consumer = []
        super().__init__(c, **kwargs)

    def start(self, c):
        """Start task consumer."""
        c.update_strategies()

        # - RabbitMQ 3.3 completely redefines how basic_qos works...
        # This will detect if the new qos semantics is in effect,
        # and if so make sure the 'apply_global' flag is set on qos updates.
        qos_global = all(not conn.qos_semantics_matches_spec for conn in c.connection)

        # set initial prefetch count
        for conn in c.connection:
            conn.default_channel.basic_qos(
                0, c.initial_prefetch_count, qos_global,
            )

            c.task_consumer.append(c.app.amqp.TaskConsumer(
                conn, on_decode_error=c.on_decode_error,
            ))

        for task_consumer in c.task_consumer:
            def set_prefetch_count(prefetch_count):
                return task_consumer.qos(
                    prefetch_count=prefetch_count,
                    apply_global=qos_global,
                )
            c.qos = QoS(set_prefetch_count, c.initial_prefetch_count)

    def stop(self, c):
        """Stop task consumer."""
        for task_consumer in c.task_consumer:
            debug('Canceling task consumer...')
            ignore_errors(c, task_consumer.cancel)

    def shutdown(self, c):
        """Shutdown task consumer."""
        if c.task_consumer:
            self.stop(c)
            for task_consumer in c.task_consumer:
                debug('Closing consumer channel...')
                ignore_errors(c, task_consumer.close)
                task_consumer = None

    def info(self, c):
        """Return task consumer info."""
        return {'prefetch_count': c.qos.value if c.qos else 'N/A'}
