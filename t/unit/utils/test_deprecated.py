from __future__ import absolute_import, unicode_literals

import pytest

from case import patch
from celery.utils import deprecated


class test_deprecated_property:

    @patch('celery.utils.deprecated.warn')
    def test_deprecated(self, warn):

        class X(object):
            _foo = None

            @deprecated.Property(deprecation='1.2')
            def foo(self):
                return self._foo

            @foo.setter
            def foo(self, value):
                self._foo = value

            @foo.deleter
            def foo(self):
                self._foo = None
        assert X.foo
        assert X.foo.__set__(None, 1)
        assert X.foo.__delete__(None)
        x = X()
        x.foo = 10
        warn.assert_called_with(
            stacklevel=3, deprecation='1.2', alternative=None,
            description='foo', removal=None,
        )
        warn.reset_mock()
        assert x.foo == 10
        warn.assert_called_with(
            stacklevel=3, deprecation='1.2', alternative=None,
            description='foo', removal=None,
        )
        warn.reset_mock()
        del(x.foo)
        warn.assert_called_with(
            stacklevel=3, deprecation='1.2', alternative=None,
            description='foo', removal=None,
        )
        assert x._foo is None

    def test_deprecated_no_setter_or_deleter(self):
        class X(object):
            @deprecated.Property(deprecation='1.2')
            def foo(self):
                pass
        assert X.foo
        x = X()
        with pytest.raises(AttributeError):
            x.foo = 10
        with pytest.raises(AttributeError):
            del(x.foo)


class test_warn:

    @patch('warnings.warn')
    def test_warn_deprecated(self, warn):
        deprecated.warn('Foo')
        warn.assert_called()
