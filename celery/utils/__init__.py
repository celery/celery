# -*- coding: utf-8 -*-
"""
    celery.utils
    ~~~~~~~~~~~~

    Utility functions.

"""
from __future__ import absolute_import, print_function, unicode_literals

import numbers
import sys
import datetime

from functools import partial

from celery.five import items, reraise, string_t

from .functional import memoize  # noqa

from .nodenames import worker_direct, nodename, nodesplit

__all__ = ['worker_direct',
           'maybe_reraise',
           'jsonify', 'gen_task_name', 'nodename', 'nodesplit',
           'cached_property']

PY3 = sys.version_info[0] == 3


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
