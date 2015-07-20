import socket

import amqp

from celery.beat.scheduler.failover.amqp_failover import AMQPFailoverStrategy

from celery.tests.case import AppCase, Mock


class test_AMQPFailoverStrategy(AppCase):
    def setup(self):
        self.app = Mock(name='source')
        self.failover_strategy = AMQPFailoverStrategy(self.app)
        self.connection = Mock()
        self.app.connection = Mock(return_value=self.connection)

    def test_is_master(self):
        self.connection.SimpleQueue = Mock()

        self.assertTrue(self.failover_strategy.is_master())
        self.connection.SimpleQueue = Mock(side_effect=amqp.exceptions.ChannelError('foo'))
        self.assertTrue(self.failover_strategy.is_master())

        self.assertIsNotNone(self.failover_strategy._throne)

    def test_is_not_master_socket_error(self):
        self.connection.SimpleQueue = Mock(side_effect=socket.error('foo'))

        self.assertFalse(self.failover_strategy.is_master())

    def test_is_not_master_connection_lost(self):
        self.connection.SimpleQueue = Mock(side_effect=amqp.exceptions.ConnectionError('foo'))

        self.assertFalse(self.failover_strategy.is_master())

    def test_is_not_master_channel_error(self):
        self.connection.SimpleQueue = Mock(side_effect=amqp.exceptions.ChannelError('foo'))

        self.assertFalse(self.failover_strategy.is_master())
