import unittest
from celery import conf
from django.conf import settings


SETTING_VARS = (
    ("CELERY_AMQP_CONSUMER_QUEUE", "AMQP_CONSUMER_QUEUE"),
    ("CELERY_AMQP_PUBLISHER_ROUTING_KEY", "AMQP_PUBLISHER_ROUTING_KEY"),
    ("CELERY_AMQP_CONSUMER_ROUTING_KEY", "AMQP_CONSUMER_ROUTING_KEY"),
    ("CELERY_AMQP_EXCHANGE_TYPE", "AMQP_EXCHANGE_TYPE"),
    ("CELERY_AMQP_EXCHANGE", "AMQP_EXCHANGE"),
    ("CELERYD_CONCURRENCY", "DAEMON_CONCURRENCY"),
    ("CELERYD_PID_FILE", "DAEMON_PID_FILE"),
    ("CELERYD_LOG_FILE", "DAEMON_LOG_FILE"),
    ("CELERYD_DAEMON_LOG_FORMAT", "LOG_FORMAT"),
)


class TestConf(unittest.TestCase):

    def assertDefaultSetting(self, setting_name, result_var):
        if hasattr(settings, setting_name):
            self.assertEquals(getattr(conf, result_var),
                              getattr(settings, setting_name),
                              "Overwritten setting %s is written to %s" % (
                                  setting_name, result_var))
        else:
            self.assertEqual(conf._DEFAULTS.get(setting_name),
                             getattr(conf, result_var),
                             "Default setting %s is written to %s" % (
                                 setting_name, result_var))

    def test_configuration_cls(self):
        for setting_name, result_var in SETTING_VARS:
            self.assertDefaultSetting(setting_name, result_var)
        self.assertTrue(isinstance(conf.DAEMON_LOG_LEVEL, int))
