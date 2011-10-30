# -*- coding: utf-8 -*-
"""
    celery.utils.functional
    ~~~~~~~~~~~~~~~~~~~~~~~

    Utilities for functions.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

from functools import wraps
from threading import Lock

try:
    from collections import Sequence
except ImportError:
    # <= Py2.5
    Sequence = (list, tuple)  # noqa

from celery.datastructures import LRUCache

KEYWORD_MARK = object()


def maybe_list(l):
    if isinstance(l, Sequence):
        return l
    return [l]


def memoize(maxsize=None, Cache=LRUCache):

    def _memoize(fun):
        mutex = Lock()
        cache = Cache(limit=maxsize)

        @wraps(fun)
        def _M(*args, **kwargs):
            key = args + (KEYWORD_MARK, ) + tuple(sorted(kwargs.iteritems()))
            try:
                with mutex:
                    value = cache[key]
            except KeyError:
                value = fun(*args, **kwargs)
                _M.misses += 1
                with mutex:
                    cache[key] = value
            else:
                _M.hits += 1
            return value

        def clear():
            """Clear the cache and reset cache statistics."""
            cache.clear()
            _M.hits = _M.misses = 0

        _M.hits = _M.misses = 0
        _M.clear = clear
        _M.original_func = fun
        return _M

    return _memoize
