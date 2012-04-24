from __future__ import absolute_import

from mock import Mock, patch

from celery.utils.imports import (
    qualname,
    symbol_by_name,
    reload_from_cwd,
    module_file,
)

from celery.tests.utils import Case


class test_import_utils(Case):

    def test_qualname(self):
        Class = type("Fox", (object, ), {"__module__": "quick.brown"})
        self.assertEqual(qualname(Class), "quick.brown.Fox")
        self.assertEqual(qualname(Class()), "quick.brown.Fox")

    def test_symbol_by_name__instance_returns_instance(self):
        instance = object()
        self.assertIs(symbol_by_name(instance), instance)

    def test_symbol_by_name_returns_default(self):
        default = object()
        self.assertIs(symbol_by_name("xyz.ryx.qedoa.weq:foz",
                        default=default), default)

    def test_symbol_by_name_package(self):
        from celery.worker import WorkController
        self.assertIs(symbol_by_name(".worker:WorkController",
                    package="celery"), WorkController)

    @patch("celery.utils.imports.reload")
    def test_reload_from_cwd(self, reload):
        reload_from_cwd("foo")
        self.assertTrue(reload.called)

    def test_reload_from_cwd_custom_reloader(self):
        reload = Mock()
        reload_from_cwd("foo", reload)
        self.assertTrue(reload.called)

    def test_module_file(self):
        m1 = Mock()
        m1.__file__ = "/opt/foo/xyz.pyc"
        self.assertEqual(module_file(m1), "/opt/foo/xyz.py")
        m2 = Mock()
        m2.__file__ = "/opt/foo/xyz.py"
        self.assertEqual(module_file(m1), "/opt/foo/xyz.py")
