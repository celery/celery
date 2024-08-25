from unittest.mock import Mock

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

        delayed_delivery = DelayedDelivery(consumer_mock)

        assert delayed_delivery.include_if(consumer_mock) is False

    def test_include_if_delivery_set_to_false_and_rabbitmq_broker(self):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery = False
        consumer_mock.app.conf.broker_url = 'amqp://'

        delayed_delivery = DelayedDelivery(consumer_mock)

        assert delayed_delivery.include_if(consumer_mock) is False

    def test_include_if_delivery_set_to_false_and_rabbitmq_broker2(self):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery = False
        consumer_mock.app.conf.broker_url = 'py-amqp://'

        delayed_delivery = DelayedDelivery(consumer_mock)

        assert delayed_delivery.include_if(consumer_mock) is False

    def test_include_if_delivery_set_to_true_and_rabbitmq_broker(self):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery = True
        consumer_mock.app.conf.broker_url = 'amqp://'

        delayed_delivery = DelayedDelivery(consumer_mock)

        assert delayed_delivery.include_if(consumer_mock) is True

    def test_include_if_delivery_set_to_true_and_rabbitmq_broker2(self):
        consumer_mock = Mock()
        consumer_mock.app.conf.broker_native_delayed_delivery = True
        consumer_mock.app.conf.broker_url = 'py-amqp://'

        delayed_delivery = DelayedDelivery(consumer_mock)

        assert delayed_delivery.include_if(consumer_mock) is True
