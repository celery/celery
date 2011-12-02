from __future__ import absolute_import
from __future__ import with_statement

import sys

from importlib import import_module

from celery.tests.utils import unittest, pypy_version, sys_platform


class test_defaults(unittest.TestCase):

    def setUp(self):
        self._prev = sys.modules.pop("celery.app.defaults", None)

    def tearDown(self):
        if self._prev:
            sys.modules["celery.app.defaults"] = self._prev

    def test_default_pool_pypy_14(self):
        with sys_platform("darwin"):
            with pypy_version((1, 4, 0)):
                self.assertEqual(self.defaults.DEFAULT_POOL, "solo")

    def test_default_pool_pypy_15(self):
        with sys_platform("darwin"):
            with pypy_version((1, 5, 0)):
                self.assertEqual(self.defaults.DEFAULT_POOL, "processes")

    def test_default_pool_jython(self):
        with sys_platform("java 1.6.51"):
            self.assertEqual(self.defaults.DEFAULT_POOL, "threads")

    @property
    def defaults(self):
        return import_module("celery.app.defaults")
