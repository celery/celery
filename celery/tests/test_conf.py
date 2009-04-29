import unittest
from celery import conf
from django.conf import settings

SETTING_VARS = (
    ("CELERY_AMQP_CONSUMER_QUEUE", "AMQP_CONSUMER_QUEUE",
        "DEFAULT_AMQP_CONSUMER_QUEUE"),
    ("CELERY_AMQP_ROUTING_KEY", "AMQP_ROUTING_KEY",
        "DEFAULT_AMQP_ROUTING_KEY"),
    ("CELERY_AMQP_EXCHANGE", "AMQP_EXCHANGE",
        "DEFAULT_AMQP_EXCHANGE"),
    ("CELERYD_CONCURRENCY", "DAEMON_CONCURRENCY",
        "DEFAULT_DAEMON_CONCURRENCY"),
    ("CELERYD_PID_FILE", "DAEMON_PID_FILE",
        "DEFAULT_DAEMON_PID_FILE"),
    ("CELERYD_EMPTY_MSG_EMIT_EVERY", "DEFAULT_EMPTY_MSG_EMIT_EVERY",
        "EMPTY_MSG_EMIT_EVERY"),
    ("CELERYD_QUEUE_WAKEUP_AFTER", "DEFAULT_QUEUE_WAKEUP_AFTER",
        "QUEUE_WAKEUP_AFTER"),
    ("CELERYD_LOG_FILE", "DEFAULT_DAEMON_LOG_FILE",
        "DAEMON_LOG_FILE"),
    ("CELERYD_DAEMON_LOG_FORMAT", "DEFAULT_LOG_FMT",
        "LOG_FORMAT"),
    ("CELERY_TASK_META_USE_DB", "DEFAULT_TASK_META_USE_DB",
        "TASK_META_USE_DB"),
)


class TestConf(unittest.TestCase):

    def assertDefaultSetting(self, setting_name, result_var, default_var):
        if hasattr(settings, setting_name):
            self.assertEquals(getattr(conf, result_var),
                              getattr(settings, setting_name),
                              "Overwritten setting %s is written to %s" % (
                                  setting_name, result_var))
        else:
            self.assertEqual(getattr(conf, default_var),
                             getattr(conf, result_var),
                             "Default setting %s is written to %s" % (
                                 default_var, result_var))
            
    def test_configuration_cls(self):
        for setting_name, result_var, default_var in SETTING_VARS:
            self.assertDefaultSetting(setting_name, result_var, default_var)
        self.assertTrue(isinstance(conf.DAEMON_LOG_LEVEL, int))
