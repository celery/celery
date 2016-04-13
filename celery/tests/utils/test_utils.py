from __future__ import absolute_import, unicode_literals

import pytz

from datetime import datetime, date, time, timedelta

from kombu import Queue

from celery.utils import (
    chunks,
    deprecated_property,
    isatty,
    is_iterable,
    cached_property,
    warn_deprecated,
    worker_direct,
    gen_task_name,
    jsonify,
)
from celery.tests.case import Case, Mock, patch


def double(x):
    return x * 2


class test_isatty(Case):

    def test_tty(self):
        fh = Mock(name='fh')
        self.assertIs(isatty(fh), fh.isatty())
        fh.isatty.side_effect = AttributeError()
        self.assertFalse(isatty(fh))


class test_worker_direct(Case):

    def test_returns_if_queue(self):
        q = Queue('foo')
        self.assertIs(worker_direct(q), q)


class test_deprecated_property(Case):

    @patch('celery.utils.warn_deprecated')
    def test_deprecated(self, warn_deprecated):

        class X(object):
            _foo = None

            @deprecated_property(deprecation='1.2')
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
        warn_deprecated.assert_called_with(
            stacklevel=3, deprecation='1.2', alternative=None,
            description='foo', removal=None,
        )
        warn_deprecated.reset_mock()
        self.assertEqual(x.foo, 10)
        warn_deprecated.assert_called_with(
            stacklevel=3, deprecation='1.2', alternative=None,
            description='foo', removal=None,
        )
        warn_deprecated.reset_mock()
        del(x.foo)
        warn_deprecated.assert_called_with(
            stacklevel=3, deprecation='1.2', alternative=None,
            description='foo', removal=None,
        )
        self.assertIsNone(x._foo)

    def test_deprecated_no_setter_or_deleter(self):
        class X(object):
            @deprecated_property(deprecation='1.2')
            def foo(self):
                pass
        self.assertTrue(X.foo)
        x = X()
        with self.assertRaises(AttributeError):
            x.foo = 10
        with self.assertRaises(AttributeError):
            del(x.foo)


class test_gen_task_name(Case):

    def test_no_module(self):
        app = Mock()
        app.name == '__main__'
        self.assertTrue(gen_task_name(app, 'foo', 'axsadaewe'))


class test_jsonify(Case):

    def test_simple(self):
        self.assertTrue(jsonify(Queue('foo')))
        self.assertTrue(jsonify(['foo', 'bar', 'baz']))
        self.assertTrue(jsonify({'foo': 'bar'}))
        self.assertTrue(jsonify(datetime.utcnow()))
        self.assertTrue(jsonify(datetime.utcnow().replace(tzinfo=pytz.utc)))
        self.assertTrue(jsonify(datetime.utcnow().replace(microsecond=0)))
        self.assertTrue(jsonify(date(2012, 1, 1)))
        self.assertTrue(jsonify(time(hour=1, minute=30)))
        self.assertTrue(jsonify(time(hour=1, minute=30, microsecond=3)))
        self.assertTrue(jsonify(timedelta(seconds=30)))
        self.assertTrue(jsonify(10))
        self.assertTrue(jsonify(10.3))
        self.assertTrue(jsonify('hello'))

        unknown_type_filter = Mock()
        obj = object()
        self.assertIs(
            jsonify(obj, unknown_type_filter=unknown_type_filter),
            unknown_type_filter.return_value,
        )
        unknown_type_filter.assert_called_with(obj)

        with self.assertRaises(ValueError):
            jsonify(obj)


class test_chunks(Case):

    def test_chunks(self):

        # n == 2
        x = chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), 2)
        self.assertListEqual(
            list(x),
            [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9], [10]],
        )

        # n == 3
        x = chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), 3)
        self.assertListEqual(
            list(x),
            [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]],
        )

        # n == 2 (exact)
        x = chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]), 2)
        self.assertListEqual(
            list(x),
            [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]],
        )


class test_utils(Case):

    def test_is_iterable(self):
        for a in 'f', ['f'], ('f',), {'f': 'f'}:
            self.assertTrue(is_iterable(a))
        for b in object(), 1:
            self.assertFalse(is_iterable(b))

    def test_cached_property(self):

        def fun(obj):
            return fun.value

        x = cached_property(fun)
        self.assertIs(x.__get__(None), x)
        self.assertIs(x.__set__(None, None), x)
        self.assertIs(x.__delete__(None), x)

    @patch('warnings.warn')
    def test_warn_deprecated(self, warn):
        warn_deprecated('Foo')
        warn.assert_called()
