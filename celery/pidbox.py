import socket
import warnings

from itertools import count

from kombu.entity import Exchange, Queue
from kombu.messaging import Consumer, Producer

from celery.app import app_or_default
from celery.utils import gen_unique_id



class Mailbox(object):

    def __init__(self, namespace, connection):
        self.namespace = namespace
        self.connection = connection
        self.exchange = Exchange("%s.pidbox" % (self.namespace, ),
                                 type="fanout",
                                 durable=False,
                                 auto_delete=True)
        self.reply_exchange = Exchange("reply.%s.pidbox" % (self.namespace, ),
                                 type="direct",
                                 durable=False,
                                 auto_delete=True)

    def publish_reply(self, reply, exchange, routing_key, channel=None):
        chan = channel or self.connection.channel()
        try:
            exchange = Exchange(exchange, exchange_type="direct",
                                          delivery_mode="transient",
                                          durable=False,
                                          auto_delete=True)
            producer = Producer(chan, exchange=exchange)
            producer.publish(reply, routing_key=routing_key)
        finally:
            channel or chan.close()

    def get_reply_queue(self, ticket):
        return Queue("%s.%s" % (ticket, self.reply_exchange.name),
                     exchange=self.reply_exchange,
                     routing_key=ticket,
                     durable=False,
                     auto_delete=True)

    def get_queue(self, hostname):
        return Queue("%s.%s.pidbox" % (hostname, self.namespace),
                     exchange=self.exchange)

    def collect_reply(self, ticket, limit=None, timeout=1,
            callback=None, channel=None):
        chan = channel or self.connection.channel()
        queue = self.get_reply_queue(ticket)
        consumer = Consumer(channel, [queue], no_ack=True)
        responses = []

        def on_message(message_data, message):
            if callback:
                callback(message_data)
            responses.append(message_data)

        try:
            consumer.register_callback(on_message)
            consumer.consume()
            for i in limit and range(limit) or count():
                try:
                    self.connection.drain_events(timeout=timeout)
                except socket.timeout:
                    break
            return responses
        finally:
            channel or chan.close()

    def publish(self, type, arguments, destination=None, reply_ticket=None,
            channel=None):
        arguments["command"] = type
        arguments["destination"] = destination
        if reply_ticket:
            arguments["reply_to"] = {"exchange": self.reply_exchange.name,
                                     "routing_key": reply_ticket}
        chan = channel or self.connection.channel()
        producer = Producer(exchange=self.exchange, delivery_mode="transient")
        try:
            producer.publish({"control": arguments})
        finally:
            channel or chan.close()

    def get_consumer(self, hostname, channel=None):
        return Consumer(channel or self.connection.channel(),
                        [self.get_queue(hostname)],
                        no_ack=True)

    def broadcast(self, command, arguments=None, destination=None,
            reply=False, timeout=1, limit=None, callback=None, channel=None):
        arguments = arguments or {}
        reply_ticket = reply and gen_unique_id() or None

        if destination is not None and \
                not isinstance(destination, (list, tuple)):
            raise ValueError("destination must be a list/tuple not %s" % (
                    type(destination)))

        # Set reply limit to number of destinations (if specificed)
        if limit is None and destination:
            limit = destination and len(destination) or None

        chan = channel or self.connection.channel()
        try:
            if reply_ticket:
                self.get_reply_queue(reply_ticket)(chan).declare()

            self.publish(command, arguments, destination=destination,
                                             reply_ticket=reply_ticket,
                                             channel=chan)

            if reply_ticket:
                return self.collect_reply(reply_ticket, limit=limit,
                                                        timeout=timeout,
                                                        callback=callback,
                                                        channel=chan)
        finally:
            channel or chan.close()


def mailbox(connection):
    return Mailbox("celeryd", connection)
