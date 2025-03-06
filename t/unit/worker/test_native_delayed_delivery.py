from logging import LogRecord
from unittest.mock import Mock, patch

import pytest
from kombu import Exchange, Queue

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

    def test_validate_broker_urls_empty(self):
        delayed_delivery = DelayedDelivery(Mock())

        with pytest.raises(ValueError, match="broker_url configuration is empty"):
            delayed_delivery._validate_broker_urls("")

        with pytest.raises(ValueError, match="broker_url configuration is empty"):
            delayed_delivery._validate_broker_urls(None)

    def test_validate_broker_urls_invalid(self):
        delayed_delivery = DelayedDelivery(Mock())

        with pytest.raises(ValueError, match="No valid broker URLs found in configuration"):
            delayed_delivery._validate_broker_urls("  ;  ;  ")

    def test_validate_broker_urls_valid(self):
        delayed_delivery = DelayedDelivery(Mock())

        urls = delayed_delivery._validate_broker_urls("amqp://localhost;amqp://remote")
        assert urls == {"amqp://localhost", "amqp://remote"}

    def test_validate_queue_type_empty(self):
        delayed_delivery = DelayedDelivery(Mock())

        with pytest.raises(ValueError, match="broker_native_delayed_delivery_queue_type is not configured"):
            delayed_delivery._validate_queue_type(None)

        with pytest.raises(ValueError, match="broker_native_delayed_delivery_queue_type is not configured"):
            delayed_delivery._validate_queue_type("")

    def test_validate_queue_type_invalid(self):
        delayed_delivery = DelayedDelivery(Mock())

        with pytest.raises(ValueError, match="Invalid queue type 'invalid'. Must be one of: classic, quorum"):
            delayed_delivery._validate_queue_type("invalid")

    def test_validate_queue_type_valid(self):
        delayed_delivery = DelayedDelivery(Mock())

        delayed_delivery._validate_queue_type("classic")
        delayed_delivery._validate_queue_type("quorum")

    @patch('celery.worker.consumer.delayed_delivery.retry_over_time')
    def test_start_retry_on_connection_error(self, mock_retry, caplog):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        consumer_mock.app.conf.broker_url = 'amqp://localhost;amqp://backup'
        consumer_mock.app.amqp.queues = {
            'celery': Queue('celery', exchange=Exchange('celery', type='topic'))
        }

        mock_retry.side_effect = ConnectionRefusedError("Connection refused")

        delayed_delivery = DelayedDelivery(consumer_mock)
        delayed_delivery.start(consumer_mock)

        # Should try both URLs
        assert mock_retry.call_count == 2
        # Should log warning for each failed attempt
        assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 2
        # Should log critical when all URLs fail
        assert len([r for r in caplog.records if r.levelname == "CRITICAL"]) == 1

    def test_on_retry_logging(self, caplog):
        delayed_delivery = DelayedDelivery(Mock())
        exc = ConnectionRefusedError("Connection refused")

        delayed_delivery._on_retry(exc, 1)

        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert record.levelname == "WARNING"
        assert "attempt 2/3" in record.message
        assert "Connection refused" in record.message

    def test_start_with_no_queues(self, caplog):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        consumer_mock.app.conf.broker_url = 'amqp://'
        consumer_mock.app.amqp.queues = {}

        delayed_delivery = DelayedDelivery(consumer_mock)
        delayed_delivery.start(consumer_mock)

        assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 1
        assert "No queues found to bind for delayed delivery" in caplog.records[0].message

    def test_start_configuration_validation_error(self, caplog):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_url = ""  # Invalid broker URL

        delayed_delivery = DelayedDelivery(consumer_mock)

        with pytest.raises(ValueError, match="broker_url configuration is empty"):
            delayed_delivery.start(consumer_mock)

        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert record.levelname == "CRITICAL"
        assert "Configuration validation failed" in record.message

    @patch('celery.worker.consumer.delayed_delivery.declare_native_delayed_delivery_exchanges_and_queues')
    def test_setup_declare_error(self, mock_declare, caplog):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        consumer_mock.app.conf.broker_url = 'amqp://'
        consumer_mock.app.amqp.queues = {
            'celery': Queue('celery', exchange=Exchange('celery', type='topic'))
        }

        mock_declare.side_effect = Exception("Failed to declare")

        delayed_delivery = DelayedDelivery(consumer_mock)
        delayed_delivery.start(consumer_mock)

        # Should log warning and critical messages
        assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 2
        assert len([r for r in caplog.records if r.levelname == "CRITICAL"]) == 1
        assert any("Failed to declare exchanges and queues" in r.message for r in caplog.records)
        assert any("Failed to setup delayed delivery for all broker URLs" in r.message for r in caplog.records)

    @patch('celery.worker.consumer.delayed_delivery.bind_queue_to_native_delayed_delivery_exchange')
    def test_setup_bind_error(self, mock_bind, caplog):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery_queue_type = 'classic'
        consumer_mock.app.conf.broker_url = 'amqp://'
        consumer_mock.app.amqp.queues = {
            'celery': Queue('celery', exchange=Exchange('celery', type='topic'))
        }

        mock_bind.side_effect = Exception("Failed to bind")

        delayed_delivery = DelayedDelivery(consumer_mock)
        delayed_delivery.start(consumer_mock)

        # Should log warning and critical messages
        assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 2
        assert len([r for r in caplog.records if r.levelname == "CRITICAL"]) == 1
        assert any("Failed to bind queue" in r.message for r in caplog.records)
        assert any("Failed to setup delayed delivery for all broker URLs" in r.message for r in caplog.records)
