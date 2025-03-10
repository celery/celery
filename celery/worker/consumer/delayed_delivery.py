from kombu.transport.native_delayed_delivery import (bind_queue_to_native_delayed_delivery_exchange,
                                                     declare_native_delayed_delivery_exchanges_and_queues)
from kombu import Connection

from celery import Celery, bootsteps
from celery.utils.log import get_logger
from celery.utils.quorum_queues import detect_quorum_queues
from celery.worker.consumer import Consumer, Tasks
from typing import Optional

__all__ = ('DelayedDelivery',)

logger = get_logger(__name__)


class DelayedDelivery(bootsteps.StartStopStep):
    """Bootstep that sets up native delayed delivery for AMQP brokers.

    This step is responsible for:
    1. Declaring the delayed delivery exchanges and queues
    2. Binding all queues to the delayed delivery exchanges
    3. Handling connection failures gracefully

    The step is only included when quorum queues are detected in the broker.
    This enables proper scheduling of ETA/countdown tasks without blocking
    worker processes.

    Note:
        This feature requires an AMQP-compliant broker that supports
        delayed message delivery through dead-letter exchanges.
    """
    requires = (Tasks,)

    def include_if(self, c: Consumer) -> bool:
        """Determine if this step should be included in the worker startup.

        Args:
            c: The consumer instance.

        Returns:
            bool: True if quorum queues are detected, False otherwise.
        """
        return detect_quorum_queues(c.app, c.app.connection_for_write().transport.driver_type)[0]

    def _setup_delayed_delivery(self, connection: Connection, app: Celery) -> None:
        """Set up delayed delivery exchanges and bindings for a single connection.

        Args:
            connection: The broker connection to set up delayed delivery for.
            app: The Celery application instance.

        Raises:
            ConnectionRefusedError: If the connection to the broker fails.
        """
        logger.debug("Setting up delayed delivery exchanges and queues")
        declare_native_delayed_delivery_exchanges_and_queues(
            connection,
            app.conf.broker_native_delayed_delivery_queue_type
        )

        logger.debug("Binding queues to delayed delivery exchanges")
        for queue in app.amqp.queues.values():
            bind_queue_to_native_delayed_delivery_exchange(connection, queue)

    def start(self, c: Consumer) -> None:
        """Start the delayed delivery setup process.

        This method attempts to set up delayed delivery on all broker URLs
        in the configuration. It continues even if some connections fail,
        to support failover scenarios.

        Args:
            c: The consumer instance.
        """
        for broker_url in c.app.conf.broker_url.split(';'):
            try:
                # We use connection for write directly to avoid using ensure_connection()
                connection = c.app.connection_for_write(url=broker_url)
                self._setup_delayed_delivery(connection, c.app)
                logger.info(f"Successfully set up delayed delivery for broker: {broker_url}")
            except ConnectionRefusedError as exc:
                logger.warning(
                    "Failed to set up delayed delivery for broker: %s. Error: %r",
                    broker_url, exc
                )

    def stop(self, c: Consumer) -> None:
        """Stop the delayed delivery service.

        Currently a no-op as there's no persistent state to clean up.

        Args:
            c: The consumer instance.
        """
        pass

    def shutdown(self, c: Consumer) -> None:
        """Shutdown the delayed delivery service.

        Currently identical to stop() as there's no additional cleanup needed.

        Args:
            c: The consumer instance.
        """
        self.stop(c)
