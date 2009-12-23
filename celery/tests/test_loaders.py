import os
import unittest

from billiard.utils.functional import wraps

from celery import loaders
from celery.loaders import base
from celery.loaders import djangoapp
from celery.tests.utils import with_environ



class TestLoaders(unittest.TestCase):

    def test_get_loader_cls(self):

        self.assertEquals(loaders.get_loader_cls("django"),
                          loaders.DjangoLoader)
        self.assertEquals(loaders.get_loader_cls("default"),
                          loaders.DefaultLoader)
        # Execute cached branch.
        self.assertEquals(loaders.get_loader_cls("django"),
                          loaders.DjangoLoader)
        self.assertEquals(loaders.get_loader_cls("default"),
                          loaders.DefaultLoader)

    @with_environ("CELERY_LOADER", "default")
    def test_detect_loader_CELERY_LOADER(self):
        self.assertEquals(loaders._detect_loader(), loaders.DefaultLoader)


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
        import sys
        self.assertEquals(sys, self.loader.import_task_module("sys"))

    def test_conf_property(self):
        self.assertEquals(self.loader.conf.foo, "bar")
        self.assertEquals(self.loader._conf_cache.foo, "bar")
        self.assertEquals(self.loader.conf.foo, "bar")

    def test_import_default_modules(self):
        import os
        import sys
        self.assertEquals(self.loader.import_default_modules(), [os, sys])


class TestDjangoLoader(unittest.TestCase):

    def setUp(self):
        self.loader = loaders.DjangoLoader()

    def test_on_worker_init(self):
        self.assertRaises(ImportError, self.loader.on_worker_init)

    def test_race_protection(self):
        djangoapp._RACE_PROTECTION = True
        try:
            self.assertFalse(self.loader.on_worker_init())
        finally:
            djangoapp._RACE_PROTECTION = False

    def test_find_related_module_no_path(self):
        self.assertFalse(djangoapp.find_related_module("sys", "tasks"))

    def test_find_related_module_no_related(self):
        self.assertFalse(djangoapp.find_related_module("someapp", "frobulators"))
