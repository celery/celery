from kombu.transport.native_delayed_delivery import (bind_queue_to_native_delayed_delivery_exchange,
                                                     declare_native_delayed_delivery_exchanges_and_queues)

from celery import Celery, bootsteps
from celery.utils.log import get_logger
from celery.utils.quorum_queues import detect_quorum_queues
from celery.worker.consumer import Consumer, Tasks

__all__ = ('DelayedDelivery',)

logger = get_logger(__name__)


class DelayedDelivery(bootsteps.StartStopStep):
    """This bootstep declares native delayed delivery queues and exchanges and binds all queues to them"""
    requires = (Tasks,)

    def include_if(self, c):
        return detect_quorum_queues(c.app, c.app.connection_for_write().transport.driver_type)[0]

    def start(self, c: Consumer):
        app: Celery = c.app

        for broker_url in app.conf.broker_url.split(';'):
            try:
                # We use connection for write directly to avoid using ensure_connection()
                connection = c.app.connection_for_write(url=broker_url)
                declare_native_delayed_delivery_exchanges_and_queues(
                    connection,
                    app.conf.broker_native_delayed_delivery_queue_type
                )

                for queue in app.amqp.queues.values():
                    bind_queue_to_native_delayed_delivery_exchange(connection, queue)
            except ConnectionRefusedError:
                # We may receive this error if a fail-over occurs
                continue
