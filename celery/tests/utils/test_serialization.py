from __future__ import absolute_import, unicode_literals

import pytz
import sys

from datetime import datetime, date, time, timedelta

from kombu import Queue

from celery.utils.serialization import (
    UnpickleableExceptionWrapper,
    get_pickleable_etype,
    jsonify,
)

from celery.tests.case import Case, Mock, mock


class test_AAPickle(Case):

    def test_no_cpickle(self):
        prev = sys.modules.pop('celery.utils.serialization', None)
        try:
            with mock.mask_modules('cPickle'):
                from celery.utils.serialization import pickle
                import pickle as orig_pickle
                self.assertIs(pickle.dumps, orig_pickle.dumps)
        finally:
            sys.modules['celery.utils.serialization'] = prev


class test_UnpickleExceptionWrapper(Case):

    def test_init(self):
        x = UnpickleableExceptionWrapper('foo', 'Bar', [10, lambda x: x])
        self.assertTrue(x.exc_args)
        self.assertEqual(len(x.exc_args), 2)


class test_get_pickleable_etype(Case):

    def test_get_pickleable_etype(self):

        class Unpickleable(Exception):
            def __reduce__(self):
                raise ValueError('foo')

        self.assertIs(get_pickleable_etype(Unpickleable), Exception)


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
