# -*- coding: utf-8 -*-
"""
    celery.utils
    ~~~~~~~~~~~~

    Utility functions.

"""
from __future__ import absolute_import, print_function, unicode_literals

import sys

from celery.five import reraise

from .functional import memoize  # noqa

from .nodenames import worker_direct, nodename, nodesplit

__all__ = ['worker_direct',
           'maybe_reraise',
           'gen_task_name', 'nodename', 'nodesplit',
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
