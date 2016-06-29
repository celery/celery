from __future__ import absolute_import, unicode_literals

from celery.tests.case import Case, patch

from celery.utils import deprecated


class test_deprecated_property(Case):

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
        self.assertTrue(X.foo)
        self.assertTrue(X.foo.__set__(None, 1))
        self.assertTrue(X.foo.__delete__(None))
        x = X()
        x.foo = 10
        warn.assert_called_with(
            stacklevel=3, deprecation='1.2', alternative=None,
            description='foo', removal=None,
        )
        warn.reset_mock()
        self.assertEqual(x.foo, 10)
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
        self.assertIsNone(x._foo)

    def test_deprecated_no_setter_or_deleter(self):
        class X(object):
            @deprecated.Property(deprecation='1.2')
            def foo(self):
                pass
        self.assertTrue(X.foo)
        x = X()
        with self.assertRaises(AttributeError):
            x.foo = 10
        with self.assertRaises(AttributeError):
            del(x.foo)


class test_warn(Case):

    @patch('warnings.warn')
    def test_warn_deprecated(self, warn):
        deprecated.warn('Foo')
        warn.assert_called()
