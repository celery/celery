from kombu import Exchange, Queue

from celery import Celery, bootsteps
from celery.utils.log import get_logger
from celery.worker.consumer import Consumer, Tasks

__all__ = ('DelayedDelivery',)

logger = get_logger(__name__)


class DelayedDelivery(bootsteps.StartStopStep):
    requires = (Tasks,)

    MAX_NUMBER_OF_BITS_TO_USE = 28
    MAX_LEVEL = MAX_NUMBER_OF_BITS_TO_USE - 1
    CELERY_DELAYED_DELIVERY_EXCHANGE = "celery_delayed_delivery"

    def include_if(self, c):
        return c.app.conf.broker_native_delayed_delivery and (
            c.app.conf.broker_url.startswith('amqp://')
            or c.app.conf.broker_url.startswith('py-amqp://')
        )

    def level_name(self, level: int) -> str:
        return f"celery_delayed_{level}"

    def start(self, c: Consumer):
        connection = c.connection_for_write()
        channel = connection.channel()

        routing_key: str = "1.#"

        for level in range(27, -1, - 1):
            current_level = self.level_name(level)
            next_level = self.level_name(level - 1)

            delayed_exchange: Exchange = Exchange(current_level, type="topic").bind(channel)
            delayed_exchange.declare()

            delayed_queue: Queue = Queue(
                current_level,
                queue_arguments={
                    "x-queue-type": "quorum",
                    "x-dead-letter-strategy": "at-least-once",
                    "x-overflow": "reject-publish",
                    "x-message-ttl": pow(2, level) * 1000,
                    "x-dead-letter-exchange": next_level if level > 0 else self.CELERY_DELAYED_DELIVERY_EXCHANGE,
                }
            ).bind(channel)
            delayed_queue.declare()
            delayed_queue.bind_to(current_level, routing_key)

            routing_key = "*." + routing_key

        routing_key = "0.#"
        for level in range(27, 0, - 1):
            current_level = self.level_name(level)
            next_level = self.level_name(level - 1)

            next_level_exchange: Exchange = Exchange(next_level, type="topic").bind(channel)

            next_level_exchange.bind_to(current_level, routing_key)

            routing_key = "*." + routing_key

        delivery_exchange: Exchange = Exchange(self.CELERY_DELAYED_DELIVERY_EXCHANGE, type="topic").bind(channel)
        delivery_exchange.declare()
        delivery_exchange.bind_to(self.level_name(0), routing_key)

        app: Celery = c.app

        for queue in app.amqp.queues.values():
            queue: Queue = queue.bind(channel)
            exchange: Exchange = queue.exchange.bind(channel)

            if exchange.type == 'direct':
                logger.warn(f"Exchange {exchange.name} is a direct exchange "
                            f"and native delayed delivery do not support direct exchanges.\n"
                            f"ETA tasks published to this exchange will block the worker until the ETA arrives.")
                continue

            routing_key = queue.routing_key if queue.routing_key.startswith('#') else f"#.{queue.routing_key}"
            exchange.bind_to(self.CELERY_DELAYED_DELIVERY_EXCHANGE, routing_key=routing_key)
            queue.bind_to(exchange.name, routing_key=routing_key)
