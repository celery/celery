import socket
import warnings

from itertools import count

from carrot.messaging import Consumer, Publisher

from celery.app import app_or_default


class ControlReplyConsumer(Consumer):
    exchange = "celerycrq"
    exchange_type = "direct"
    durable = False
    exclusive = False
    auto_delete = True
    no_ack = True

    def __init__(self, connection, ticket, **kwargs):
        self.ticket = ticket
        queue = "%s.%s" % (self.exchange, ticket)
        super(ControlReplyConsumer, self).__init__(connection,
                                                   queue=queue,
                                                   routing_key=ticket,
                                                   **kwargs)

    def collect(self, limit=None, timeout=1, callback=None):
        responses = []

        def on_message(message_data, message):
            if callback:
                callback(message_data)
            responses.append(message_data)

        self.callbacks = [on_message]
        self.consume()
        for i in limit and range(limit) or count():
            try:
                self.connection.drain_events(timeout=timeout)
            except socket.timeout:
                break

        return responses


class ControlReplyPublisher(Publisher):
    exchange = "celerycrq"
    exchange_type = "direct"
    delivery_mode = "non-persistent"
    durable = False
    auto_delete = True


class BroadcastPublisher(Publisher):
    """Publish broadcast commands"""

    ReplyTo = ControlReplyConsumer

    def __init__(self, *args, **kwargs):
        app = self.app = app_or_default(kwargs.get("app"))
        kwargs["exchange"] = kwargs.get("exchange") or \
                                app.conf.CELERY_BROADCAST_EXCHANGE
        kwargs["exchange_type"] = kwargs.get("exchange_type") or \
                                app.conf.CELERY_BROADCAST_EXCHANGE_TYPE
        super(BroadcastPublisher, self).__init__(*args, **kwargs)

    def send(self, type, arguments, destination=None, reply_ticket=None):
        """Send broadcast command."""
        arguments["command"] = type
        arguments["destination"] = destination
        reply_to = self.ReplyTo(self.connection, None, app=self.app,
                                auto_declare=False)
        if reply_ticket:
            arguments["reply_to"] = {"exchange": reply_to.exchange,
                                     "routing_key": reply_ticket}
        super(BroadcastPublisher, self).send({"control": arguments})


class BroadcastConsumer(Consumer):
    """Consume broadcast commands"""
    no_ack = True

    def __init__(self, *args, **kwargs):
        self.app = app = app_or_default(kwargs.get("app"))
        kwargs["queue"] = kwargs.get("queue") or \
                            app.conf.CELERY_BROADCAST_QUEUE
        kwargs["exchange"] = kwargs.get("exchange") or \
                            app.conf.CELERY_BROADCAST_EXCHANGE
        kwargs["exchange_type"] = kwargs.get("exchange_type") or \
                            app.conf.CELERY_BROADCAST_EXCHANGE_TYPE
        self.hostname = kwargs.pop("hostname", None) or socket.gethostname()
        self.queue = "%s_%s" % (self.queue, self.hostname)
        super(BroadcastConsumer, self).__init__(*args, **kwargs)

    def verify_exclusive(self):
        # XXX Kombu material
        channel = getattr(self.backend, "channel")
        if channel and hasattr(channel, "queue_declare"):
            try:
                _, _, consumers = channel.queue_declare(self.queue,
                                                        passive=True)
            except ValueError:
                pass
            else:
                if consumers:
                    warnings.warn(UserWarning(
                        "A node named %s is already using this process "
                        "mailbox. Maybe you should specify a custom name "
                        "for this node with the -n argument?" % self.hostname))

    def consume(self, *args, **kwargs):
        self.verify_exclusive()
        return super(BroadcastConsumer, self).consume(*args, **kwargs)
