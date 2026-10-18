import json
import pickle
import sys
from datetime import date, datetime, time, timedelta, timezone
from unittest.mock import Mock

import pytest
from kombu import Queue

from celery.utils.serialization import (STRTOBOOL_DEFAULT_TABLE, UnpickleableExceptionWrapper, ensure_serializable,
                                        get_pickleable_etype, get_pickleable_exception, jsonify, strtobool)

try:
    ExceptionGroup
except NameError:  # Python < 3.11
    from exceptiongroup import ExceptionGroup


class UnpickleableError(Exception):
    """Exception that cannot be pickled (it holds a lambda)."""

    def __init__(self, message='unpickleable'):
        super().__init__(message)
        self.callback = lambda x: x


class AggregateError(Exception):
    """Unpickleable exception carrying an ``exceptions`` tuple.

    Not an exception group: it only happens to aggregate other
    exceptions in an attribute named ``exceptions``.
    """

    def __init__(self, message='aggregate'):
        super().__init__(message)
        self.exceptions = (ValueError('member'),)
        self.callback = lambda x: x


class UnderivableGroup(ExceptionGroup):
    """Exception group whose derived groups remain unpickleable."""

    def derive(self, subgroup):
        return ExceptionGroup('group', [UnpickleableError(), *subgroup])


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


class test_get_pickleable_exception:

    def test_pickleable_exception_returned_unchanged(self):
        exc = ValueError('foo')
        assert get_pickleable_exception(exc) is exc

    def test_unpickleable_exception_wrapped(self):
        result = get_pickleable_exception(UnpickleableError())
        assert isinstance(result, UnpickleableExceptionWrapper)

    def test_pickleable_exception_group_returned_unchanged(self):
        exc = ExceptionGroup('group', [ValueError('foo'), TypeError('bar')])
        assert get_pickleable_exception(exc) is exc

    def test_unpickleable_group_keeps_group_replacing_members(self):
        member = ValueError('foo')
        exc = ExceptionGroup('group', [UnpickleableError(), member])
        result = get_pickleable_exception(exc)
        # Only the unpickleable member is replaced; the group itself,
        # its message and the pickleable member are kept.
        assert isinstance(result, ExceptionGroup)
        assert result.args[0] == 'group'
        assert isinstance(result.exceptions[0],
                          UnpickleableExceptionWrapper)
        assert result.exceptions[1] is member
        # The returned group must round-trip through pickle.
        restored = pickle.loads(pickle.dumps(result))
        assert isinstance(restored, ExceptionGroup)
        assert restored.exceptions[1].args == member.args

    def test_unpickleable_group_searches_nested_groups(self):
        member = KeyError('foo')
        exc = ExceptionGroup('group', [
            ExceptionGroup('nested', [UnpickleableError(), member]),
        ])
        result = get_pickleable_exception(exc)
        assert isinstance(result, ExceptionGroup)
        nested = result.exceptions[0]
        assert isinstance(nested, ExceptionGroup)
        assert isinstance(nested.exceptions[0],
                          UnpickleableExceptionWrapper)
        assert nested.exceptions[1] is member

    def test_unpickleable_group_falls_back_to_first_pickleable_member(self):
        member = ValueError('foo')
        exc = UnderivableGroup('group', [UnpickleableError(), member])
        assert get_pickleable_exception(exc) is member

    def test_group_without_pickleable_members_keeps_group_of_wrappers(self):
        exc = ExceptionGroup('group', [UnpickleableError(), UnpickleableError()])
        result = get_pickleable_exception(exc)
        assert isinstance(result, ExceptionGroup)
        assert len(result.exceptions) == 2
        assert all(isinstance(subexc, UnpickleableExceptionWrapper)
                   for subexc in result.exceptions)
        # The returned group must round-trip through pickle.
        pickle.loads(pickle.dumps(result))

    def test_non_group_with_exceptions_attribute_is_wrapped(self):
        result = get_pickleable_exception(AggregateError())
        assert isinstance(result, UnpickleableExceptionWrapper)
        assert result.exc_cls_name == 'AggregateError'


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
        datetime.now(timezone.utc),
        datetime.now(timezone.utc).replace(tzinfo=ZoneInfo("UTC")),
        datetime.now(timezone.utc).replace(microsecond=0),
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
