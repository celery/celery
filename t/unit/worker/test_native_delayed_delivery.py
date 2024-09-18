from logging import LogRecord
from unittest.mock import Mock

from kombu import Exchange, Queue

from celery.worker.consumer.delayed_delivery import DelayedDelivery


class test_DelayedDelivery:
    def test_include_if_delivery_set_to_false(self):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery = False

        delayed_delivery = DelayedDelivery(consumer_mock)

        assert delayed_delivery.include_if(consumer_mock) is False

    def test_include_if_delivery_set_to_false_and_not_rabbitmq_broker(self):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery = False
        consumer_mock.app.conf.broker_url = 'redis://'
        consumer_mock.connection.transport.driver_type = 'redis'

        delayed_delivery = DelayedDelivery(consumer_mock)

        assert delayed_delivery.include_if(consumer_mock) is False

    def test_include_if_delivery_set_to_false_and_rabbitmq_broker(self):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery = False
        consumer_mock.app.conf.broker_url = 'amqp://'
        consumer_mock.connection.transport.driver_type = 'amqp'

        delayed_delivery = DelayedDelivery(consumer_mock)

        assert delayed_delivery.include_if(consumer_mock) is False

    def test_include_if_delivery_set_to_false_and_rabbitmq_broker2(self):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery = False
        consumer_mock.app.conf.broker_url = 'py-amqp://'
        consumer_mock.connection.transport.driver_type = 'amqp'

        delayed_delivery = DelayedDelivery(consumer_mock)

        assert delayed_delivery.include_if(consumer_mock) is False

    def test_include_if_delivery_set_to_true_and_rabbitmq_broker(self):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery = True
        consumer_mock.app.conf.broker_url = 'amqp://'
        consumer_mock.connection.transport.driver_type = 'amqp'

        delayed_delivery = DelayedDelivery(consumer_mock)

        assert delayed_delivery.include_if(consumer_mock) is True

    def test_include_if_delivery_set_to_true_and_rabbitmq_broker2(self):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery = True
        consumer_mock.app.conf.broker_url = 'py-amqp://'
        consumer_mock.connection.transport.driver_type = 'amqp'

        delayed_delivery = DelayedDelivery(consumer_mock)

        assert delayed_delivery.include_if(consumer_mock) is True

    def test_start_native_delayed_delivery_direct_exchange(self, caplog):
        consumer_mock = Mock()
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
        consumer_mock.app.amqp.queues = {
            'celery': Queue('celery', exchange=Exchange('celery', type='topic'))
        }

        delayed_delivery = DelayedDelivery(consumer_mock)

        delayed_delivery.start(consumer_mock)

        assert len(caplog.records) == 0

    def test_start_native_delayed_delivery_fanout_exchange(self, caplog):
        consumer_mock = Mock()
        consumer_mock.app.amqp.queues = {
            'celery': Queue('celery', exchange=Exchange('celery', type='fanout'))
        }

        delayed_delivery = DelayedDelivery(consumer_mock)

        delayed_delivery.start(consumer_mock)

        assert len(caplog.records) == 0
