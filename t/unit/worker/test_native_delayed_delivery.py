from logging import LogRecord
from unittest.mock import Mock, patch

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
