# -*- coding: utf-8 -*-
"""Streaming, truncating, non-recursive version of :func:`repr`.

Differences from regular :func:`repr`:

- Sets are represented the Python 3 way: ``{1, 2}`` vs ``set([1, 2])``.
- Unicode strings does not have the ``u'`` prefix, even on Python 2.
- Empty set formatted as ``set()`` (Python 3), not ``set([])`` (Python 2).
- Longs don't have the ``L`` suffix.

Very slow with no limits, super quick with limits.
"""
from __future__ import absolute_import, unicode_literals

import sys

from collections import Iterable, Mapping, deque, namedtuple

from decimal import Decimal
from itertools import chain
from numbers import Number
from pprint import _recursion

from kombu.utils.encoding import bytes_to_str

from celery.five import items, text_t

from .text import truncate, truncate_bytes

__all__ = ['saferepr', 'reprstream']

# pylint: disable=redefined-outer-name
# We cache globals and attribute lookups, so disable this warning.

IS_PY3 = sys.version_info[0] == 3

if IS_PY3:  # pragma: no cover
    range_t = (range, )
else:
    class range_t(object):  # noqa
        pass

_literal = namedtuple('_literal', ('value', 'truncate', 'direction'))
_key = namedtuple('_key', ('value',))
_quoted = namedtuple('_quoted', ('value',))
_dirty = namedtuple('_dirty', ('objid',))

chars_t = (bytes, text_t)
safe_t = (Number,)
set_t = (frozenset, set)

LIT_DICT_START = _literal('{', False, +1)
LIT_DICT_KVSEP = _literal(': ', True, 0)
LIT_DICT_END = _literal('}', False, -1)
LIT_LIST_START = _literal('[', False, +1)
LIT_LIST_END = _literal(']', False, -1)
LIT_LIST_SEP = _literal(', ', True, 0)
LIT_SET_START = _literal('{', False, +1)
LIT_SET_END = _literal('}', False, -1)
LIT_TUPLE_START = _literal('(', False, +1)
LIT_TUPLE_END = _literal(')', False, -1)
LIT_TUPLE_END_SV = _literal(',)', False, -1)


def saferepr(o, maxlen=None, maxlevels=3, seen=None):
    """Safe version of :func:`repr`.

    Warning:
        Make sure you set the maxlen argument, or it will be very slow
        for recursive objects.  With the maxlen set, it's often faster
        than built-in repr.
    """
    return ''.join(_saferepr(
        o, maxlen=maxlen, maxlevels=maxlevels, seen=seen
    ))


def _chaindict(mapping,
               LIT_DICT_KVSEP=LIT_DICT_KVSEP,
               LIT_LIST_SEP=LIT_LIST_SEP):
    size = len(mapping)
    for i, (k, v) in enumerate(items(mapping)):
        yield _key(k)
        yield LIT_DICT_KVSEP
        yield v
        if i < (size - 1):
            yield LIT_LIST_SEP


def _chainlist(it, LIT_LIST_SEP=LIT_LIST_SEP):
    size = len(it)
    for i, v in enumerate(it):
        yield v
        if i < (size - 1):
            yield LIT_LIST_SEP


def _repr_empty_set(s):
    return '%s()' % (type(s).__name__,)


def _saferepr(o, maxlen=None, maxlevels=3, seen=None):
    stack = deque([iter([o])])
    for token, it in reprstream(stack, seen=seen, maxlevels=maxlevels):
        if maxlen is not None and maxlen <= 0:
            yield ', ...'
            # move rest back to stack, so that we can include
            # dangling parens.
            stack.append(it)
            break
        if isinstance(token, _literal):
            val = token.value
        elif isinstance(token, _key):
            val = saferepr(token.value, maxlen, maxlevels)
        elif isinstance(token, _quoted):
            val = token.value
            if IS_PY3 and isinstance(val, bytes):  # pragma: no cover
                val = "b'%s'" % (bytes_to_str(truncate_bytes(val, maxlen)),)
            else:
                val = "'%s'" % (truncate(val, maxlen),)
        else:
            val = truncate(token, maxlen)
        yield val
        if maxlen is not None:
            maxlen -= len(val)
    for rest1 in stack:
        # maxlen exceeded, process any dangling parens.
        for rest2 in rest1:
            if isinstance(rest2, _literal) and not rest2.truncate:
                yield rest2.value


def _reprseq(val, lit_start, lit_end, builtin_type, chainer):
    if type(val) is builtin_type:  # noqa
        return lit_start, lit_end, chainer(val)
    return (
        _literal('%s(%s' % (type(val).__name__, lit_start.value), False, +1),
        _literal('%s)' % (lit_end.value,), False, -1),
        chainer(val)
    )


def reprstream(stack, seen=None, maxlevels=3, level=0, isinstance=isinstance):
    """Streaming repr, yielding tokens."""
    seen = seen or set()
    append = stack.append
    popleft = stack.popleft
    is_in_seen = seen.__contains__
    discard_from_seen = seen.discard
    add_to_seen = seen.add

    while stack:
        lit_start = lit_end = None
        it = popleft()
        for val in it:
            orig = val
            if isinstance(val, _dirty):
                discard_from_seen(val.objid)
                continue
            elif isinstance(val, _literal):
                level += val.direction
                yield val, it
            elif isinstance(val, _key):
                yield val, it
            elif isinstance(val, Decimal):
                yield repr(val), it
            elif isinstance(val, safe_t):
                yield text_t(val), it
            elif isinstance(val, chars_t):
                yield _quoted(val), it
            elif isinstance(val, range_t):  # pragma: no cover
                yield repr(val), it
            else:
                if isinstance(val, set_t):
                    if not val:
                        yield _repr_empty_set(val), it
                        continue
                    lit_start, lit_end, val = _reprseq(
                        val, LIT_SET_START, LIT_SET_END, set, _chainlist,
                    )
                elif isinstance(val, tuple):
                    lit_start, lit_end, val = (
                        LIT_TUPLE_START,
                        LIT_TUPLE_END_SV if len(val) == 1 else LIT_TUPLE_END,
                        _chainlist(val))
                elif isinstance(val, Mapping):
                    lit_start, lit_end, val = (
                        LIT_DICT_START, LIT_DICT_END, _chaindict(val))
                elif isinstance(val, Iterable):
                    lit_start, lit_end, val = (
                        LIT_LIST_START, LIT_LIST_END, _chainlist(val))
                else:
                    # other type of object
                    yield repr(val), it
                    continue

                if maxlevels and level >= maxlevels:
                    yield '%s...%s' % (lit_start.value, lit_end.value), it
                    continue

                objid = id(orig)
                if is_in_seen(objid):
                    yield _recursion(orig), it
                    continue
                add_to_seen(objid)

                # Recurse into the new list/tuple/dict/etc by tacking
                # the rest of our iterable onto the new it: this way
                # it works similar to a linked list.
                append(chain([lit_start], val, [_dirty(objid), lit_end], it))
                break
