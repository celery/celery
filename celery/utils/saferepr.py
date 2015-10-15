# -*- coding: utf-8 -*-
"""
    celery.utils.saferepr
    ~~~~~~~~~~~~~~~~~~~~~

    Streaming, truncating, non-recursive version of :func:`repr`.

    Differences from regular :func:`repr`:

    - Sets are represented the Python 3 way: ``{1, 2}`` vs ``set([1, 2])``.
    - Unicode strings does not have the ``u'`` prefix, even on Python 2.

    Very slow with no limits, super quick with limits.

"""
from collections import Iterable, Mapping, deque, namedtuple

from itertools import chain
from numbers import Number
from pprint import _recursion

from celery.five import items, text_t

from .text import truncate

__all__ = ['saferepr']

_literal = namedtuple('_literal', ('value', 'truncate', 'direction'))
_key = namedtuple('_key', ('value',))
_quoted = namedtuple('_quoted', ('value',))
_dirty = namedtuple('_dirty', ('objid',))

chars_t = (bytes, text_t)
literal_t = (_literal, _key)
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
    return '%s([])' % (type(s).__name__,)


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
            val = str(token.value)
        elif isinstance(token, _key):
            val = repr(token.value).replace("u'", "'")
        elif isinstance(token, _quoted):
            val = "'%s'" % (truncate(token.value, maxlen),)
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


def reprstream(stack, seen=None, maxlevels=3, level=0, isinstance=isinstance):
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
            elif isinstance(val, safe_t):
                yield repr(val), it
            elif isinstance(val, chars_t):
                yield _quoted(val), it
            else:
                if isinstance(val, set_t):
                    if not val:
                        yield _repr_empty_set(val), it
                        continue
                    lit_start, lit_end, val = (
                        LIT_SET_START, LIT_SET_END, _chainlist(val))
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
                    yield "%s...%s" % (lit_start.value, lit_end.value), it
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
