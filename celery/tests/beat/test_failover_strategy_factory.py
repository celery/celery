from celery.app.utils import Settings
from celery.tests.case import Mock

from celery.beat.scheduler.failover.amqp_failover import AMQPFailoverStrategy
from celery.beat.scheduler.failover.failover_strategy_factory import FailoverStrategyFactory
from celery.tests.case import AppCase


class test_FailoverStrategyFactory(AppCase):
    def setup(self):
        self.app = Mock(main='source', conf=Settings({}, []))
        self.connection = Mock()
        self.app.connection = Mock(return_value=self.connection)

    def test_with_broker_url(self):
        content_type = "amqp"
        self.app.conf["BROKER_URL"] = "%s://blah_blah//" % content_type
        factory = FailoverStrategyFactory(self.app)
        self.assertEqual(0, len(factory.strategies.keys()))
        failover_strategy = factory.get_failover_strategy()
        self.assertEqual(1, len(factory.strategies.keys()))
        self.assertEqual(factory.strategies.get("%s.%s" % (self.app.main, content_type)), failover_strategy)
        self.assertEqual(factory.strategies.get("%s.%s" % (self.app.main, content_type)), failover_strategy)
        self.assertTrue(isinstance(failover_strategy, AMQPFailoverStrategy))

        # Test for second time because of caching mechanism
        failover_strategy = factory.get_failover_strategy()
        self.assertEqual(1, len(factory.strategies.keys()))
        self.assertEqual(factory.strategies.get("%s.%s" % (self.app.main, content_type)), failover_strategy)
        self.assertTrue(isinstance(failover_strategy, AMQPFailoverStrategy))


    def test_with_failover_broker_url(self):
        self.app.conf["BROKER_URL"] = "redis://blah_blah//"

        content_type = "amqp"
        self.app.conf["CELERYBEAT_FAILOVER_BROKER_URL"] = "%s://blah_blah//" % content_type

        factory = FailoverStrategyFactory(self.app)
        self.assertEqual(0, len(factory.strategies.keys()))
        failover_strategy = factory.get_failover_strategy()
        self.assertEqual(1, len(factory.strategies.keys()))
        self.assertEqual(factory.strategies.get("%s.%s" % (self.app.main, content_type)), failover_strategy)
        self.assertEqual(factory.strategies.get("%s.%s" % (self.app.main, content_type)), failover_strategy)
        self.assertTrue(isinstance(failover_strategy, AMQPFailoverStrategy))

        # Test for second time because of caching mechanism
        failover_strategy = factory.get_failover_strategy()
        self.assertEqual(1, len(factory.strategies.keys()))
        self.assertEqual(factory.strategies.get("%s.%s" % (self.app.main, content_type)), failover_strategy)
        self.assertTrue(isinstance(failover_strategy, AMQPFailoverStrategy))
