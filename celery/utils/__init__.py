# -*- coding: utf-8 -*-
"""
    celery.utils
    ~~~~~~~~~~~~

    Utility functions.

"""
from __future__ import absolute_import, print_function, unicode_literals

import numbers
import os
import sys
import traceback
import datetime

from functools import partial
from pprint import pprint

from celery.five import WhateverIO, items, reraise, string_t

from .functional import memoize  # noqa

from .nodenames import worker_direct, nodename, nodesplit

__all__ = ['worker_direct', 'lpmerge',
           'is_iterable', 'isatty', 'cry', 'maybe_reraise', 'strtobool',
           'jsonify', 'gen_task_name', 'nodename', 'nodesplit',
           'cached_property']

PY3 = sys.version_info[0] == 3

#: Billiard sets this when execv is enabled.
#: We use it to find out the name of the original ``__main__``
#: module, so that we can properly rewrite the name of the
#: task to be that of ``App.main``.
MP_MAIN_FILE = os.environ.get('MP_MAIN_FILE')


def lpmerge(L, R):
    """In place left precedent dictionary merge.

    Keeps values from `L`, if the value in `R` is :const:`None`."""
    setitem = L.__setitem__
    [setitem(k, v) for k, v in items(R) if v is not None]
    return L


def is_iterable(obj):
    try:
        iter(obj)
    except TypeError:
        return False
    return True


def isatty(fh):
    try:
        return fh.isatty()
    except AttributeError:
        pass


def cry(out=None, sepchr='=', seplen=49):  # pragma: no cover
    """Return stack-trace of all active threads,
    taken from https://gist.github.com/737056."""
    import threading

    out = WhateverIO() if out is None else out
    P = partial(print, file=out)

    # get a map of threads by their ID so we can print their names
    # during the traceback dump
    tmap = {t.ident: t for t in threading.enumerate()}

    sep = sepchr * seplen
    for tid, frame in items(sys._current_frames()):
        thread = tmap.get(tid)
        if not thread:
            # skip old junk (left-overs from a fork)
            continue
        P('{0.name}'.format(thread))
        P(sep)
        traceback.print_stack(frame, file=out)
        P(sep)
        P('LOCAL VARIABLES')
        P(sep)
        pprint(frame.f_locals, stream=out)
        P('\n')
    return out.getvalue()


def maybe_reraise():
    """Re-raise if an exception is currently being handled, or return
    otherwise."""
    exc_info = sys.exc_info()
    try:
        if exc_info[2]:
            reraise(exc_info[0], exc_info[1], exc_info[2])
    finally:
        # see http://docs.python.org/library/sys.html#sys.exc_info
        del(exc_info)


def strtobool(term, table={'false': False, 'no': False, '0': False,
                           'true': True, 'yes': True, '1': True,
                           'on': True, 'off': False}):
    """Convert common terms for true/false to bool
    (true/false/yes/no/on/off/1/0)."""
    if isinstance(term, string_t):
        try:
            return table[term.lower()]
        except KeyError:
            raise TypeError('Cannot coerce {0!r} to type bool'.format(term))
    return term


def jsonify(obj,
            builtin_types=(numbers.Real, string_t), key=None,
            keyfilter=None,
            unknown_type_filter=None):
    """Transforms object making it suitable for json serialization"""
    from kombu.abstract import Object as KombuDictType
    _jsonify = partial(jsonify, builtin_types=builtin_types, key=key,
                       keyfilter=keyfilter,
                       unknown_type_filter=unknown_type_filter)

    if isinstance(obj, KombuDictType):
        obj = obj.as_dict(recurse=True)

    if obj is None or isinstance(obj, builtin_types):
        return obj
    elif isinstance(obj, (tuple, list)):
        return [_jsonify(v) for v in obj]
    elif isinstance(obj, dict):
        return {
            k: _jsonify(v, key=k) for k, v in items(obj)
            if (keyfilter(k) if keyfilter else 1)
        }
    elif isinstance(obj, datetime.datetime):
        # See "Date Time String Format" in the ECMA-262 specification.
        r = obj.isoformat()
        if obj.microsecond:
            r = r[:23] + r[26:]
        if r.endswith('+00:00'):
            r = r[:-6] + 'Z'
        return r
    elif isinstance(obj, datetime.date):
        return obj.isoformat()
    elif isinstance(obj, datetime.time):
        r = obj.isoformat()
        if obj.microsecond:
            r = r[:12]
        return r
    elif isinstance(obj, datetime.timedelta):
        return str(obj)
    else:
        if unknown_type_filter is None:
            raise ValueError(
                'Unsupported type: {0!r} {1!r} (parent: {2})'.format(
                    type(obj), obj, key))
        return unknown_type_filter(obj)


# ------------------------------------------------------------------------ #
# > XXX Compat
from .log import LOG_LEVELS     # noqa
from .imports import (          # noqa
    qualname as get_full_cls_name, symbol_by_name as get_cls_by_name,
    instantiate, import_from_cwd, gen_task_name,
)
from .functional import chunks, noop                    # noqa
from kombu.utils import cached_property, uuid   # noqa
gen_unique_id = uuid
