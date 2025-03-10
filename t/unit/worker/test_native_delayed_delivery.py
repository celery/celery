from logging import LogRecord
from unittest.mock import Mock, patch, call

from kombu import Exchange, Queue
from kombu.transport.native_delayed_delivery import bind_queue_to_native_delayed_delivery_exchange

from celery.worker.consumer.delayed_delivery import DelayedDelivery


class test_DelayedDelivery:
    @patch('celery.worker.consumer.delayed_delivery.detect_quorum_queues', return_value=[False, ""])
    def test_include_if_no_quorum_queues_detected(self, detect_quorum_queues):
        consumer_mock = Mock()

        delayed_delivery = DelayedDelivery(consumer_mock)

        assert delayed_delivery.include_if(consumer_mock) is False

    @patch('celery.worker.consumer.delayed_delivery.detect_quorum_queues', return_value=[True, ""])
    def test_include_if_quorum_queues_detected(self, detect_quorum_queues):
        consumer_mock = Mock()

        delayed_delivery = DelayedDelivery(consumer_mock)

        assert delayed_delivery.include_if(consumer_mock) is True

    def test_start_native_delayed_delivery_direct_exchange(self, caplog):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        consumer_mock.app.conf.broker_url = 'amqp://'
        consumer_mock.app.amqp.queues = {
            'celery': Queue('celery', exchange=Exchange('celery', type='direct'))
        }

        delayed_delivery = DelayedDelivery(consumer_mock)

        delayed_delivery.start(consumer_mock)

        assert len(caplog.records) == 1
        record: LogRecord = caplog.records[0]
        assert record.levelname == "WARNING"
        assert record.message == (
            "Exchange celery is a direct exchange "
            "and native delayed delivery do not support direct exchanges.\n"
            "ETA tasks published to this exchange "
            "will block the worker until the ETA arrives."
        )

    def test_start_native_delayed_delivery_topic_exchange(self, caplog):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        consumer_mock.app.conf.broker_url = 'amqp://'
        consumer_mock.app.amqp.queues = {
            'celery': Queue('celery', exchange=Exchange('celery', type='topic'))
        }

        delayed_delivery = DelayedDelivery(consumer_mock)

        delayed_delivery.start(consumer_mock)

        assert len(caplog.records) == 0

    def test_start_native_delayed_delivery_fanout_exchange(self, caplog):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        consumer_mock.app.conf.broker_url = 'amqp://'
        consumer_mock.app.amqp.queues = {
            'celery': Queue('celery', exchange=Exchange('celery', type='fanout'))
        }

        delayed_delivery = DelayedDelivery(consumer_mock)

        delayed_delivery.start(consumer_mock)

        assert len(caplog.records) == 0

    @patch('celery.worker.consumer.delayed_delivery.logger')
    def test_start_native_delayed_delivery_multiple_brokers(self, mock_logger):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        consumer_mock.app.conf.broker_url = 'amqp://broker1;amqp://broker2'
        consumer_mock.app.amqp.queues = {
            'celery': Queue(
                'celery',
                routing_key='celery',
                exchange=Exchange('celery', type='topic')
            )
        }

        delayed_delivery = DelayedDelivery(consumer_mock)

        # Mock connection_for_write to simulate different broker behaviors
        def connection_for_write(url=None):
            if url == 'amqp://broker2':
                raise ConnectionRefusedError("Connection refused")
            connection = Mock()
            connection.transport = Mock()
            connection.transport.driver_type = 'amqp'
            return connection

        consumer_mock.app.connection_for_write = connection_for_write

        delayed_delivery.start(consumer_mock)

        # Verify warning was logged
        assert mock_logger.warning.call_count == 1
        args = mock_logger.warning.call_args[0]
        assert args[0] == "Failed to set up delayed delivery for broker: %s. Error: %r"
        assert args[1] == 'amqp://broker2'
        assert isinstance(args[2], ConnectionRefusedError)
        assert str(args[2]) == "Connection refused"

    @patch('celery.worker.consumer.delayed_delivery.bind_queue_to_native_delayed_delivery_exchange')
    def test_start_native_delayed_delivery_multiple_queues(self, mock_bind_queue):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        consumer_mock.app.conf.broker_url = 'amqp://'
        
        # Set up multiple queues
        queues = {
            'celery': Queue('celery', exchange=Exchange('celery', type='topic')),
            'high_priority': Queue('high_priority', exchange=Exchange('high_priority', type='topic')),
            'low_priority': Queue('low_priority', exchange=Exchange('low_priority', type='fanout'))
        }
        consumer_mock.app.amqp.queues = queues

        delayed_delivery = DelayedDelivery(consumer_mock)
        connection = Mock()
        consumer_mock.app.connection_for_write.return_value = connection

        delayed_delivery.start(consumer_mock)

        # Verify each queue was bound
        assert mock_bind_queue.call_count == len(queues)
        for queue in queues.values():
            mock_bind_queue.assert_any_call(connection, queue)

    def test_start_native_delayed_delivery_empty_broker_url(self):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        consumer_mock.app.conf.broker_url = ''
        consumer_mock.app.amqp.queues = {
            'celery': Queue('celery', exchange=Exchange('celery', type='topic'))
        }

        delayed_delivery = DelayedDelivery(consumer_mock)
        delayed_delivery.start(consumer_mock)

        # Empty URL should be treated as a single empty string, so one connection attempt is expected
        assert consumer_mock.app.connection_for_write.call_count == 1

    @patch('celery.worker.consumer.delayed_delivery.logger')
    def test_start_native_delayed_delivery_invalid_broker_url(self, mock_logger):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        consumer_mock.app.conf.broker_url = 'invalid://broker'
        consumer_mock.app.amqp.queues = {
            'celery': Queue('celery', exchange=Exchange('celery', type='topic'))
        }

        delayed_delivery = DelayedDelivery(consumer_mock)
        consumer_mock.app.connection_for_write.side_effect = ValueError("Invalid URL")

        delayed_delivery.start(consumer_mock)

        # Verify warning was logged
        assert mock_logger.warning.call_count == 1
        args = mock_logger.warning.call_args[0]
        assert args[0] == "Failed to set up delayed delivery for broker: %s. Error: %r"
        assert args[1] == 'invalid://broker'
        assert isinstance(args[2], ValueError)
        assert str(args[2]) == "Invalid URL"

    @patch('celery.worker.consumer.delayed_delivery.detect_quorum_queues')
    def test_include_if_no_connection(self, mock_detect_quorum_queues):
        consumer_mock = Mock()
        transport_mock = Mock()
        transport_mock.driver_type = 'amqp'
        connection_mock = Mock()
        connection_mock.transport = transport_mock
        consumer_mock.app.connection_for_write.return_value = connection_mock

        delayed_delivery = DelayedDelivery(consumer_mock)
        mock_detect_quorum_queues.return_value = [False, ""]

        # Should return False when no quorum queues are detected
        assert delayed_delivery.include_if(consumer_mock) is False
