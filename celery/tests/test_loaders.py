import os
import sys
import unittest2 as unittest

from celery import task
from celery import loaders
from celery.loaders import base
from celery.loaders import default

from celery.tests.utils import with_environ



class TestLoaders(unittest.TestCase):

    def test_get_loader_cls(self):

        self.assertEqual(loaders.get_loader_cls("default"),
                          loaders.DefaultLoader)
        # Execute cached branch.
        self.assertEqual(loaders.get_loader_cls("default"),
                          loaders.DefaultLoader)

    @with_environ("CELERY_LOADER", "default")
    def test_detect_loader_CELERY_LOADER(self):
        self.assertEqual(loaders.detect_loader(), loaders.DefaultLoader)


class DummyLoader(base.BaseLoader):

    class Config(object):

        def __init__(self, **kwargs):
            for attr, val in kwargs.items():
                setattr(self, attr, val)

    def read_configuration(self):
        return self.Config(foo="bar", CELERY_IMPORTS=("os", "sys"))


class TestLoaderBase(unittest.TestCase):

    def setUp(self):
        self.loader = DummyLoader()

    def test_handlers_pass(self):
        self.loader.on_task_init("foo.task", "feedface-cafebabe")
        self.loader.on_worker_init()

    def test_import_task_module(self):
        self.assertEqual(sys, self.loader.import_task_module("sys"))

    def test_conf_property(self):
        self.assertEqual(self.loader.conf.foo, "bar")
        self.assertEqual(self.loader._conf_cache.foo, "bar")
        self.assertEqual(self.loader.conf.foo, "bar")

    def test_import_default_modules(self):
        self.assertItemsEqual(self.loader.import_default_modules(),
                              [os, sys, task])


def modifies_django_env(fun):

    def _protected(*args, **kwargs):
        from django.conf import settings
        current = dict((key, getattr(settings, key))
                        for key in settings.get_all_members()
                            if key.isupper())
        try:
            return fun(*args, **kwargs)
        finally:
            for key, value in current.items():
                setattr(settings, key, value)

    return _protected


class TestDefaultLoader(unittest.TestCase):

    def test_wanted_module_item(self):
        self.assertTrue(default.wanted_module_item("FOO"))
        self.assertTrue(default.wanted_module_item("foo"))
        self.assertFalse(default.wanted_module_item("_foo"))
        self.assertFalse(default.wanted_module_item("__foo"))

    @modifies_django_env
    def test_read_configuration(self):
        from types import ModuleType

        class ConfigModule(ModuleType):
            pass

        celeryconfig = ConfigModule("celeryconfig")
        celeryconfig.CELERY_IMPORTS = ("os", "sys")

        sys.modules["celeryconfig"] = celeryconfig
        try:
            l = default.Loader()
            settings = l.read_configuration()
            self.assertTupleEqual(settings.CELERY_IMPORTS, ("os", "sys"))
            from django.conf import settings
            settings.configured = False
            settings = l.read_configuration()
            self.assertTupleEqual(settings.CELERY_IMPORTS, ("os", "sys"))
            self.assertTrue(settings.configured)
            l.on_worker_init()
        finally:
            sys.modules.pop("celeryconfig", None)
