from __future__ import absolute_import, unicode_literals

import re

from decimal import Decimal
from pprint import pprint

from celery.five import items, long_t, values

from celery.utils.saferepr import saferepr

from celery.tests.case import Case

D_NUMBERS = {
    b'integer': 1,
    b'float': 1.3,
    b'decimal': Decimal("1.3"),
    b'long': long_t(1.3),
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

RE_OLD_SET_REPR = re.compile(r'(?:frozen)?set\d?\(\[(.+?)\]\)')
RE_OLD_SET_REPR_REPLACE = r'{\1}'


def from_old_repr(s):
    return RE_OLD_SET_REPR.sub(
        RE_OLD_SET_REPR_REPLACE, s).replace("u'", "'")


class list2(list):
    pass


class list3(list):

    def __repr__(self):
        return list.__repr__(self)


class tuple2(tuple):
    pass


class tuple3(tuple):

    def __repr__(self):
        return tuple.__repr__(self)


class set2(set):
    pass


class set3(set):

    def __repr__(self):
        return set.__repr__(self)


class frozenset2(frozenset):
    pass


class frozenset3(frozenset):

    def __repr__(self):
        return frozenset.__repr__(self)


class dict2(dict):
    pass


class dict3(dict):

    def __repr__(self):
        return dict.__repr__(self)


class Unorderable:

    def __repr__(self):
        return str(id(self))


class test_saferepr(Case):

    def test_safe_types(self):
        for value in values(D_NUMBERS):
            self.assertEqual(saferepr(value), repr(value))

    def test_numbers_dict(self):
        self.assertEqual(saferepr(D_NUMBERS), repr(D_NUMBERS))

    def test_numbers_list(self):
        self.assertEqual(saferepr(L_NUMBERS), repr(L_NUMBERS))

    def test_numbers_keys(self):
        self.assertEqual(saferepr(D_INT_KEYS), repr(D_INT_KEYS))

    def test_text(self):
        self.assertEqual(saferepr(D_TEXT), repr(D_TEXT).replace("u'", "'"))

    def test_text_maxlen(self):
        self.assertEqual(saferepr(D_D_TEXT, 100),
                from_old_repr(repr(D_D_TEXT)[:99] + "...', ...}}"))

    def test_same_as_repr(self):
        # Simple objects, small containers and classes that overwrite __repr__
        # For those the result should be the same as repr().
        # Ahem.  The docs don't say anything about that -- this appears to
        # be testing an implementation quirk.  Starting in Python 2.5, it's
        # not true for dicts:  pprint always sorts dicts by key now; before,
        # it sorted a dict display if and only if the display required
        # multiple lines.  For that reason, dicts with more than one element
        # aren't tested here.
        types = (
            0, 0, 0+0j, 0.0, "", b"",
            (), tuple2(), tuple3(),
            [], list2(), list3(),
            set(), set2(), set3(),
            frozenset(), frozenset2(), frozenset3(),
            {}, dict2(), dict3(),
            self.assertTrue, pprint,
            -6, -6, -6-6j, -1.5, "x", b"x", (3,), [3], {3: 6},
            (1, 2), [3, 4], {5: 6},
            tuple2((1, 2)), tuple3((1, 2)), tuple3(range(100)),
            [3, 4], list2([3, 4]), list3([3, 4]), list3(range(100)),
            set({7}), set2({7}), set3({7}),
            frozenset({8}), frozenset2({8}), frozenset3({8}),
            dict2({5: 6}), dict3({5: 6}),
            range(10, -11, -1)
        )
        for simple in types:
            native = from_old_repr(repr(simple))
            self.assertEqual(saferepr(simple), native)
