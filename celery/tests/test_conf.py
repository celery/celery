import unittest
from celery import conf
from django.conf import settings


SETTING_VARS = (
    ("CELERY_DEFAULT_QUEUE", "DEFAULT_QUEUE"),
    ("CELERY_DEFAULT_ROUTING_KEY", "DEFAULT_ROUTING_KEY"),
    ("CELERY_DEFAULT_EXCHANGE_TYPE", "DEFAULT_EXCHANGE_TYPE"),
    ("CELERY_DEFAULT_EXCHANGE", "DEFAULT_EXCHANGE"),
    ("CELERYD_CONCURRENCY", "CELERYD_CONCURRENCY"),
    ("CELERYD_LOG_FILE", "CELERYD_LOG_FILE"),
    ("CELERYD_LOG_FORMAT", "CELERYD_LOG_FORMAT"),
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
        self.assertTrue(isinstance(conf.CELERYD_LOG_LEVEL, int))
