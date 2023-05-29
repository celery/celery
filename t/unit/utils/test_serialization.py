import json
import pickle
import sys
from datetime import date, datetime, time, timedelta
from unittest.mock import Mock

import pytest
from kombu import Queue

from celery.utils.serialization import (STRTOBOOL_DEFAULT_TABLE, UnpickleableExceptionWrapper, ensure_serializable,
                                        get_pickleable_etype, jsonify, strtobool)

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo


class test_AAPickle:

    @pytest.mark.masked_modules('cPickle')
    def test_no_cpickle(self, mask_modules):
        prev = sys.modules.pop('celery.utils.serialization', None)
        try:
            import pickle as orig_pickle

            from celery.utils.serialization import pickle
            assert pickle.dumps is orig_pickle.dumps
        finally:
            sys.modules['celery.utils.serialization'] = prev


class test_ensure_serializable:

    def test_json_py3(self):
        expected = (1, "<class 'object'>")
        actual = ensure_serializable([1, object], encoder=json.dumps)
        assert expected == actual

    def test_pickle(self):
        expected = (1, object)
        actual = ensure_serializable(expected, encoder=pickle.dumps)
        assert expected == actual


class test_UnpickleExceptionWrapper:

    def test_init(self):
        x = UnpickleableExceptionWrapper('foo', 'Bar', [10, lambda x: x])
        assert x.exc_args
        assert len(x.exc_args) == 2


class test_get_pickleable_etype:

    def test_get_pickleable_etype(self):
        class Unpickleable(Exception):
            def __reduce__(self):
                raise ValueError('foo')

        assert get_pickleable_etype(Unpickleable) is Exception


class test_jsonify:

    @pytest.mark.parametrize('obj', [
        Queue('foo'),
        ['foo', 'bar', 'baz'],
        {'foo': 'bar'},
        datetime.utcnow(),
        datetime.utcnow().replace(tzinfo=ZoneInfo("UTC")),
        datetime.utcnow().replace(microsecond=0),
        date(2012, 1, 1),
        time(hour=1, minute=30),
        time(hour=1, minute=30, microsecond=3),
        timedelta(seconds=30),
        10,
        10.3,
        'hello',
    ])
    def test_simple(self, obj):
        assert jsonify(obj)

    def test_unknown_type_filter(self):
        unknown_type_filter = Mock()
        obj = object()
        assert (jsonify(obj, unknown_type_filter=unknown_type_filter) is
                unknown_type_filter.return_value)
        unknown_type_filter.assert_called_with(obj)

        with pytest.raises(ValueError):
            jsonify(obj)


class test_strtobool:

    @pytest.mark.parametrize('s,b',
                             STRTOBOOL_DEFAULT_TABLE.items())
    def test_default_table(self, s, b):
        assert strtobool(s) == b

    def test_unknown_value(self):
        with pytest.raises(TypeError, match="Cannot coerce 'foo' to type bool"):
            strtobool('foo')

    def test_no_op(self):
        assert strtobool(1) == 1

    def test_custom_table(self):
        custom_table = {
            'foo': True,
            'bar': False
        }

        assert strtobool("foo", table=custom_table)
        assert not strtobool("bar", table=custom_table)
