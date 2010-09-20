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
                          default.Loader)

    @with_environ("CELERY_LOADER", "default")
    def test_detect_loader_CELERY_LOADER(self):
        self.assertIsInstance(loaders.setup_loader(), default.Loader)


class DummyLoader(base.BaseLoader):

    def read_configuration(self):
        return {"foo": "bar", "CELERY_IMPORTS": ("os", "sys")}


class TestLoaderBase(unittest.TestCase):

    def setUp(self):
        self.loader = DummyLoader()

    def test_handlers_pass(self):
        self.loader.on_task_init("foo.task", "feedface-cafebabe")
        self.loader.on_worker_init()

    def test_import_task_module(self):
        self.assertEqual(sys, self.loader.import_task_module("sys"))

    def test_conf_property(self):
        self.assertEqual(self.loader.conf["foo"], "bar")
        self.assertEqual(self.loader._conf_cache["foo"], "bar")
        self.assertEqual(self.loader.conf["foo"], "bar")

    def test_import_default_modules(self):
        self.assertItemsEqual(self.loader.import_default_modules(),
                              [os, sys, task])


class TestDefaultLoader(unittest.TestCase):

    def test_wanted_module_item(self):
        self.assertTrue(default.wanted_module_item("FOO"))
        self.assertTrue(default.wanted_module_item("Foo"))
        self.assertFalse(default.wanted_module_item("_FOO"))
        self.assertFalse(default.wanted_module_item("__FOO"))
        self.assertFalse(default.wanted_module_item("foo"))

    def test_read_configuration(self):
        from types import ModuleType

        class ConfigModule(ModuleType):
            pass

        celeryconfig = ConfigModule("celeryconfig")
        celeryconfig.CELERY_IMPORTS = ("os", "sys")
        configname = os.environ.get("CELERY_CONFIG_MODULE") or "celeryconfig"

        prevconfig = sys.modules[configname]
        sys.modules[configname] = celeryconfig
        try:
            l = default.Loader()
            settings = l.read_configuration()
            self.assertTupleEqual(settings.CELERY_IMPORTS, ("os", "sys"))
            settings = l.read_configuration()
            self.assertTupleEqual(settings.CELERY_IMPORTS, ("os", "sys"))
            l.on_worker_init()
        finally:
            sys.modules[configname] = prevconfig
