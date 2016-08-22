from __future__ import absolute_import, unicode_literals

import pytest

from case import Mock

from celery.five import bytes_if_py2
from celery.utils.imports import (
    NotAPackage,
    qualname,
    gen_task_name,
    reload_from_cwd,
    module_file,
    find_module,
)


def test_find_module():
    assert find_module('celery')
    imp = Mock()
    imp.return_value = None
    with pytest.raises(NotAPackage):
        find_module('foo.bar.baz', imp=imp)
    assert find_module('celery.worker.request')


def test_qualname():
    Class = type(bytes_if_py2('Fox'), (object,), {
        '__module__': 'quick.brown',
    })
    assert qualname(Class) == 'quick.brown.Fox'
    assert qualname(Class()) == 'quick.brown.Fox'


def test_reload_from_cwd(patching):
    reload = patching('celery.utils.imports.reload')
    reload_from_cwd('foo')
    reload.assert_called()


def test_reload_from_cwd_custom_reloader():
    reload = Mock()
    reload_from_cwd('foo', reload)
    reload.assert_called()


def test_module_file():
    m1 = Mock()
    m1.__file__ = '/opt/foo/xyz.pyc'
    assert module_file(m1) == '/opt/foo/xyz.py'
    m2 = Mock()
    m2.__file__ = '/opt/foo/xyz.py'
    assert module_file(m1) == '/opt/foo/xyz.py'


class test_gen_task_name:

    def test_no_module(self):
        app = Mock()
        app.name == '__main__'
        assert gen_task_name(app, 'foo', 'axsadaewe')
