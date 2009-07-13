import unittest
from django.conf import settings
from celery.discovery import autodiscover
from celery.task import tasks


class TestDiscovery(unittest.TestCase):

    def assertDiscovery(self):
        apps = autodiscover()
        self.assertTrue(apps)
        tasks.autodiscover()
        self.assertTrue("c.unittest.SomeAppTask" in tasks)
        self.assertEquals(tasks["c.unittest.SomeAppTask"].run(), 42)

    def test_discovery(self):
        if "someapp" in settings.INSTALLED_APPS:
            self.assertDiscovery()

    def test_discovery_with_broken(self):
        if "someapp" in settings.INSTALLED_APPS:
            settings.INSTALLED_APPS = settings.INSTALLED_APPS + ["xxxnot.aexist"]
            self.assertDiscovery()
