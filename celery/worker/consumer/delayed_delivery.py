from kombu.transport.native_delayed_delivery import (bind_queue_to_native_delayed_delivery_exchange,
                                                     declare_native_delayed_delivery_exchanges_and_queues)

from celery import Celery, bootsteps
from celery.utils.log import get_logger
from celery.worker.consumer import Consumer, Tasks

__all__ = ('DelayedDelivery',)

logger = get_logger(__name__)


class DelayedDelivery(bootsteps.StartStopStep):
    """This bootstep declares native delayed delivery queues and exchanges and binds all queues to them"""
    requires = (Tasks,)

    def include_if(self, c):
        return (c.app.conf.broker_native_delayed_delivery
                and c.connection_for_write().transport.driver_type == 'amqp')

    def start(self, c: Consumer):
        connection = c.connection_for_write()
        app: Celery = c.app

        declare_native_delayed_delivery_exchanges_and_queues(
            connection,
            app.conf.broker_native_delayed_delivery_queue_type
        )

        for queue in app.amqp.queues.values():
            bind_queue_to_native_delayed_delivery_exchange(connection, queue)
