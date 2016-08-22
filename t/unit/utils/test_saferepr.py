from __future__ import absolute_import, unicode_literals

import pytest
import re

from decimal import Decimal
from pprint import pprint

from celery.five import (
    items, long_t, python_2_unicode_compatible, text_t, values,
)

from celery.utils.saferepr import saferepr

D_NUMBERS = {
    b'integer': 1,
    b'float': 1.3,
    b'decimal': Decimal('1.3'),
    b'long': long_t(4),
    b'complex': complex(13.3),
}
D_INT_KEYS = {v: k for k, v in items(D_NUMBERS)}

QUICK_BROWN_FOX = 'The quick brown fox jumps over the lazy dog.'
B_QUICK_BROWN_FOX = b'The quick brown fox jumps over the lazy dog.'

D_TEXT = {
    b'foo': QUICK_BROWN_FOX,
    b'bar': B_QUICK_BROWN_FOX,
    b'baz': B_QUICK_BROWN_FOX,
    b'xuzzy': B_QUICK_BROWN_FOX,
}

L_NUMBERS = list(values(D_NUMBERS))

D_TEXT_LARGE = {
    b'bazxuzzyfoobarlongverylonglong': QUICK_BROWN_FOX * 30,
}

D_ALL = {
    b'numbers': D_NUMBERS,
    b'intkeys': D_INT_KEYS,
    b'text': D_TEXT,
    b'largetext': D_TEXT_LARGE,
}

D_D_TEXT = {b'rest': D_TEXT}

RE_OLD_SET_REPR = re.compile(r'(?<!frozen)set\([\[|\{](.+?)[\}\]]\)')
RE_OLD_SET_REPR_REPLACE = r'{\1}'
RE_OLD_SET_CUSTOM_REPR = re.compile(r'((?:frozen)?set\d?\()\[(.+?)\](\))')
RE_OLD_SET_CUSTOM_REPR_REPLACE = r'\1{\2}\3'
RE_EMPTY_SET_REPR = re.compile(r'((?:frozen)?set\d?)\(\[\]\)')
RE_EMPTY_SET_REPR_REPLACE = r'\1()'
RE_LONG_SUFFIX = re.compile(r'(\d)+L')


def old_repr(s):
    return text_t(RE_LONG_SUFFIX.sub(
        r'\1',
        RE_EMPTY_SET_REPR.sub(
            RE_EMPTY_SET_REPR_REPLACE,
            RE_OLD_SET_REPR.sub(
                RE_OLD_SET_REPR_REPLACE,
                RE_OLD_SET_CUSTOM_REPR.sub(
                    RE_OLD_SET_CUSTOM_REPR_REPLACE, repr(s).replace("u'", "'"),
                )
            ),
        ),
    )).replace('set([])', 'set()')


class list2(list):
    pass


@python_2_unicode_compatible
class list3(list):

    def __repr__(self):
        return list.__repr__(self)


class tuple2(tuple):
    pass


@python_2_unicode_compatible
class tuple3(tuple):

    def __repr__(self):
        return tuple.__repr__(self)


class set2(set):
    pass


@python_2_unicode_compatible
class set3(set):

    def __repr__(self):
        return set.__repr__(self)


class frozenset2(frozenset):
    pass


@python_2_unicode_compatible
class frozenset3(frozenset):

    def __repr__(self):
        return frozenset.__repr__(self)


class dict2(dict):
    pass


@python_2_unicode_compatible
class dict3(dict):

    def __repr__(self):
        return dict.__repr__(self)


class test_saferepr:

    @pytest.mark.parametrize('value', list(values(D_NUMBERS)))
    def test_safe_types(self, value):
        assert saferepr(value) == old_repr(value)

    def test_numbers_dict(self):
        assert saferepr(D_NUMBERS) == old_repr(D_NUMBERS)

    def test_numbers_list(self):
        assert saferepr(L_NUMBERS) == old_repr(L_NUMBERS)

    def test_numbers_keys(self):
        assert saferepr(D_INT_KEYS) == old_repr(D_INT_KEYS)

    def test_text(self):
        assert saferepr(D_TEXT) == old_repr(D_TEXT).replace("u'", "'")

    def test_text_maxlen(self):
        assert saferepr(D_D_TEXT, 100).endswith("...', ...}}")

    def test_maxlevels(self):
        saferepr(D_ALL, maxlevels=1)

    def test_recursion(self):
        d = {1: 2, 3: {4: 5}}
        d[3][6] = d
        res = saferepr(d)
        assert 'Recursion on' in res

    @pytest.mark.parametrize('value', [
        0, 0, 0 + 0j, 0.0, '', b'',
        (), tuple2(), tuple3(),
        [], list2(), list3(),
        set(), set2(), set3(),
        frozenset(), frozenset2(), frozenset3(),
        {}, dict2(), dict3(),
        test_recursion, pprint,
        -6, -6, -6 - 6j, -1.5, 'x', b'x', (3,), [3], {3: 6},
        (1, 2), [3, 4], {5: 6},
        tuple2((1, 2)), tuple3((1, 2)), tuple3(range(100)),
        [3, 4], list2([3, 4]), list3([3, 4]), list3(range(100)),
        {7}, set2({7}), set3({7}),
        frozenset({8}), frozenset2({8}), frozenset3({8}),
        dict2({5: 6}), dict3({5: 6}),
        range(10, -11, -1)
    ])
    def test_same_as_repr(self, value):
        # Simple objects, small containers, and classes that overwrite __repr__
        # For those the result should be the same as repr().
        # Ahem.  The docs don't say anything about that -- this appears to
        # be testing an implementation quirk.  Starting in Python 2.5, it's
        # not true for dicts:  pprint always sorts dicts by key now; before,
        # it sorted a dict display if and only if the display required
        # multiple lines.  For that reason, dicts with more than one element
        # aren't tested here.
        native = old_repr(value)
        assert saferepr(value) == native
