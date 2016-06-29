from __future__ import absolute_import, unicode_literals

import pytz

from datetime import datetime, date, time, timedelta

from kombu import Queue

from celery.utils import (
    chunks,
    isatty,
    is_iterable,
    cached_property,
    jsonify,
)
from celery.tests.case import Case, Mock


class test_isatty(Case):

    def test_tty(self):
        fh = Mock(name='fh')
        self.assertIs(isatty(fh), fh.isatty())
        fh.isatty.side_effect = AttributeError()
        self.assertFalse(isatty(fh))


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
