import unittest2 as unittest

from django.conf import settings

from celery.loaders.djangoapp import autodiscover
from celery.task import tasks


class TestDiscovery(unittest.TestCase):

    def assertDiscovery(self):
        apps = autodiscover()
        self.assertTrue(apps)
        self.assertIn("c.unittest.SomeAppTask", tasks)
        self.assertEqual(tasks["c.unittest.SomeAppTask"].run(), 42)

    def test_discovery(self):
        if "someapp" in settings.INSTALLED_APPS:
            self.assertDiscovery()

    def test_discovery_with_broken(self):
        if "someapp" in settings.INSTALLED_APPS:
            installed_apps = list(settings.INSTALLED_APPS)
            settings.INSTALLED_APPS = installed_apps + ["xxxnot.aexist"]
            try:
                self.assertRaises(ImportError, autodiscover)
            finally:
                settings.INSTALLED_APPS = installed_apps
