"""
    celery.beat.scheduler.failover.amqp_failover
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    RabbitMQ failover strategy to make Celery Beat high available with broker RabbitMQ
"""

import socket
from kombu import BrokerConnection
from celery.beat.scheduler.failover.base_failover import BaseFailoverStrategy
from celery.utils.log import get_logger

import amqp


logger = get_logger(__name__)


class AMQPFailoverStrategy(BaseFailoverStrategy):
    """
        Creates an locks with and mutex queue which other nodes can not connect the queue as Consumer. This guaranties that
        the node is master or not.
    """

    connection_type = "amqp"

    def __init__(self, app, *args, **kwargs):
        super(AMQPFailoverStrategy, self).__init__(app, *args, **kwargs)
        self.name = app.main
        self._conn = None
        self._throne = None

    def is_master(self):
        return self._fight_for_throne()


    @property
    def _connection(self):
        if self._conn == None:
            if self.failover_broker_url:
                self._conn = BrokerConnection(self.failover_broker_url)
            else:
                self._conn = self.app.connection()
        return self._conn


    def _fight_for_throne(self):
        if self._throne:
            return True
        else:
            try:
                self._throne = self._connection.SimpleQueue('%s.mutex' % self.name, queue_opts={'exclusive': True})
                return True
            except (socket.error, amqp.exceptions.ConnectionError):
                logger.warning('Connection lost!')
            except amqp.exceptions.ChannelError:
                logger.debug("Not the %s master", self.name)
            return False