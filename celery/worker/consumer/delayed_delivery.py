"""Native delayed delivery functionality for Celery workers.

This module provides the DelayedDelivery bootstep which handles setup and configuration
of native delayed delivery functionality when using quorum queues.
"""
from typing import Iterator, List, Optional, Set, Union, ValuesView

# Backport of PEP 654 for Python versions < 3.11
# In Python 3.11+, exceptiongroup uses the built-in ExceptionGroup
from exceptiongroup import ExceptionGroup
from kombu import Connection, Queue
from kombu.transport.native_delayed_delivery import (bind_queue_to_native_delayed_delivery_exchange,
                                                     declare_native_delayed_delivery_exchanges_and_queues)
from kombu.utils.functional import retry_over_time
from kombu.utils.url import maybe_sanitize_url

from celery import Celery, bootsteps
from celery.utils.log import get_logger
from celery.utils.quorum_queues import detect_quorum_queues
from celery.worker.consumer import Consumer, Tasks

__all__ = ('DelayedDelivery',)

logger = get_logger(__name__)


# Default retry settings
RETRY_INTERVAL = 1.0  # seconds between retries
MAX_RETRIES = 3      # maximum number of retries
RETRIED_EXCEPTIONS = (ConnectionRefusedError, OSError)

# Valid queue types for delayed delivery
VALID_QUEUE_TYPES = {'classic', 'quorum'}


class DelayedDelivery(bootsteps.StartStopStep):
    """Bootstep that sets up native delayed delivery functionality.

    This component handles the setup and configuration of native delayed delivery
    for Celery workers. It is automatically included when quorum queues are
    detected in the application configuration.

    Responsibilities:
        - Declaring native delayed delivery exchanges and queues
        - Binding all application queues to the delayed delivery exchanges
        - Handling connection failures gracefully with retries
        - Validating configuration settings
    """

    requires = (Tasks,)

    def include_if(self, c: Consumer) -> bool:
        """Determine if this bootstep should be included.

        Args:
            c: The Celery consumer instance

        Returns:
            bool: True if quorum queues are detected, False otherwise
        """
        return detect_quorum_queues(c.app, c.app.connection_for_write().transport.driver_type)[0]

    def start(self, c: Consumer) -> None:
        """Initialize delayed delivery for all broker URLs.

        Attempts to set up delayed delivery for each broker URL in the configuration.
        Failures are logged but don't prevent attempting remaining URLs.

        Args:
            c: The Celery consumer instance

        Raises:
            ValueError: If configuration validation fails
        """
        app: Celery = c.app

        try:
            self._validate_configuration(app)
        except ValueError as e:
            logger.critical("Configuration validation failed: %s", str(e))
            raise

        broker_urls = self._validate_broker_urls(app.conf.broker_url)
        setup_errors = []

        for broker_url in broker_urls:
            try:
                retry_over_time(
                    self._setup_delayed_delivery,
                    args=(c, broker_url),
                    catch=RETRIED_EXCEPTIONS,
                    errback=self._on_retry,
                    interval_start=RETRY_INTERVAL,
                    max_retries=MAX_RETRIES,
                )
            except Exception as e:
                logger.warning(
                    "Failed to setup delayed delivery for %r: %s",
                    maybe_sanitize_url(broker_url), str(e)
                )
                setup_errors.append((broker_url, e))

        if len(setup_errors) == len(broker_urls):
            logger.critical(
                "Failed to setup delayed delivery for all broker URLs. "
                "Native delayed delivery will not be available."
            )

    def _setup_delayed_delivery(self, c: Consumer, broker_url: str) -> None:
        """Set up delayed delivery for a specific broker URL.

        Args:
            c: The Celery consumer instance
            broker_url: The broker URL to configure

        Raises:
            ConnectionRefusedError: If connection to the broker fails
            OSError: If there are network-related issues
            Exception: For other unexpected errors during setup
        """
        with c.app.connection_for_write(url=broker_url) as connection:
            queue_type = c.app.conf.broker_native_delayed_delivery_queue_type
            logger.debug(
                "Setting up delayed delivery for broker %r with queue type %r",
                maybe_sanitize_url(broker_url), queue_type
            )

            try:
                declare_native_delayed_delivery_exchanges_and_queues(
                    connection,
                    queue_type
                )
            except Exception as e:
                logger.warning(
                    "Failed to declare exchanges and queues for %r: %s",
                    maybe_sanitize_url(broker_url), str(e)
                )
                raise

            try:
                self._bind_queues(c.app, connection)
            except Exception as e:
                logger.warning(
                    "Failed to bind queues for %r: %s",
                    maybe_sanitize_url(broker_url), str(e)
                )
                raise

    def _bind_queues(self, app: Celery, connection: Connection) -> None:
        """Bind all application queues to delayed delivery exchanges.

        Args:
            app: The Celery application instance
            connection: The broker connection to use

        Raises:
            Exception: If queue binding fails
        """
        queues: ValuesView[Queue] = app.amqp.queues.values()
        if not queues:
            logger.warning("No queues found to bind for delayed delivery")
            return

        exceptions: list[Exception] = []
        for queue in queues:
            try:
                logger.debug("Binding queue %r to delayed delivery exchange", queue.name)
                bind_queue_to_native_delayed_delivery_exchange(connection, queue)
            except Exception as e:
                logger.error(
                    "Failed to bind queue %r: %s",
                    queue.name, str(e)
                )

                # We must re-raise on retried exceptions to ensure they are
                # caught with the outer retry_over_time mechanism.
                #
                # This could be removed if one of:
                # * The minimum python version for Celery and Kombu is
                #   increased to 3.11. Kombu updated to use the `except*`
                #   clause to catch specific exceptions from an ExceptionGroup.
                # * Kombu's retry_over_time utility is updated to use the
                #   catch utility from agronholm's exceptiongroup backport.
                if isinstance(e, RETRIED_EXCEPTIONS):
                    raise

                exceptions.append(e)

        if exceptions:
            raise ExceptionGroup(
                ("One or more failures occurred while binding queues to "
                 "delayed delivery exchanges"),
                exceptions,
            )

    def _on_retry(self, exc: Exception, interval_range: Iterator[float], intervals_count: int) -> float:
        """Callback for retry attempts.

        Args:
            exc: The exception that triggered the retry
            interval_range: An iterator which returns the time in seconds to sleep next
            intervals_count: Number of retry attempts so far
        """
        interval = next(interval_range)
        logger.warning(
            "Retrying delayed delivery setup (attempt %d/%d) after error: %s. Sleeping %.2f seconds.",
            intervals_count + 1, MAX_RETRIES, str(exc), interval
        )
        return interval

    def _validate_configuration(self, app: Celery) -> None:
        """Validate all required configuration settings.

        Args:
            app: The Celery application instance

        Raises:
            ValueError: If any configuration is invalid
        """
        # Validate broker URLs
        self._validate_broker_urls(app.conf.broker_url)

        # Validate queue type
        self._validate_queue_type(app.conf.broker_native_delayed_delivery_queue_type)

    def _validate_broker_urls(self, broker_urls: Union[str, List[str]]) -> Set[str]:
        """Validate and split broker URLs.

        Args:
            broker_urls: Broker URLs, either as a semicolon-separated string
                  or as a list of strings

        Returns:
            Set of valid broker URLs

        Raises:
            ValueError: If no valid broker URLs are found or if invalid URLs are provided
        """
        if not broker_urls:
            raise ValueError("broker_url configuration is empty")

        if isinstance(broker_urls, str):
            brokers = broker_urls.split(";")
        elif isinstance(broker_urls, list):
            if not all(isinstance(url, str) for url in broker_urls):
                raise ValueError("All broker URLs must be strings")
            brokers = broker_urls
        else:
            raise ValueError(f"broker_url must be a string or list, got {broker_urls!r}")

        valid_urls = {url for url in brokers}

        if not valid_urls:
            raise ValueError("No valid broker URLs found in configuration")

        return valid_urls

    def _validate_queue_type(self, queue_type: Optional[str]) -> None:
        """Validate the queue type configuration.

        Args:
            queue_type: The configured queue type

        Raises:
            ValueError: If queue type is invalid
        """
        if not queue_type:
            raise ValueError("broker_native_delayed_delivery_queue_type is not configured")

        if queue_type not in VALID_QUEUE_TYPES:
            sorted_types = sorted(VALID_QUEUE_TYPES)
            raise ValueError(
                f"Invalid queue type {queue_type!r}. Must be one of: {', '.join(sorted_types)}"
            )
