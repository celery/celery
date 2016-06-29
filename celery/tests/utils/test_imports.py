from __future__ import absolute_import, unicode_literals

from celery.five import bytes_if_py2

from celery.utils.imports import (
    NotAPackage,
    qualname,
    gen_task_name,
    reload_from_cwd,
    module_file,
    find_module,
)

from celery.tests.case import Case, Mock, patch


class test_import_utils(Case):

    def test_find_module(self):
        self.assertTrue(find_module('celery'))
        imp = Mock()
        imp.return_value = None
        with self.assertRaises(NotAPackage):
            find_module('foo.bar.baz', imp=imp)
        self.assertTrue(find_module('celery.worker.request'))

    def test_qualname(self):
        Class = type(bytes_if_py2('Fox'), (object,), {
            '__module__': 'quick.brown',
        })
        self.assertEqual(qualname(Class), 'quick.brown.Fox')
        self.assertEqual(qualname(Class()), 'quick.brown.Fox')

    @patch('celery.utils.imports.reload')
    def test_reload_from_cwd(self, reload):
        reload_from_cwd('foo')
        reload.assert_called()

    def test_reload_from_cwd_custom_reloader(self):
        reload = Mock()
        reload_from_cwd('foo', reload)
        reload.assert_called()

    def test_module_file(self):
        m1 = Mock()
        m1.__file__ = '/opt/foo/xyz.pyc'
        self.assertEqual(module_file(m1), '/opt/foo/xyz.py')
        m2 = Mock()
        m2.__file__ = '/opt/foo/xyz.py'
        self.assertEqual(module_file(m1), '/opt/foo/xyz.py')


class test_gen_task_name(Case):

    def test_no_module(self):
        app = Mock()
        app.name == '__main__'
        self.assertTrue(gen_task_name(app, 'foo', 'axsadaewe'))
