# -*- coding: utf-8 -*-
"""Utility functions.

Don't import from here directly anymore, as these are only
here for backwards compatibility.
"""
from __future__ import absolute_import, print_function, unicode_literals

import sys

from .functional import memoize  # noqa

from .nodenames import worker_direct, nodename, nodesplit

__all__ = ['worker_direct', 'gen_task_name', 'nodename', 'nodesplit',
           'cached_property', 'uuid']

PY3 = sys.version_info[0] == 3


# ------------------------------------------------------------------------ #
# > XXX Compat
from .log import LOG_LEVELS     # noqa
from .imports import (          # noqa
    qualname as get_full_cls_name, symbol_by_name as get_cls_by_name,
    instantiate, import_from_cwd, gen_task_name,
)
from .functional import chunks, noop                    # noqa
from kombu.utils.objects import cached_property         # noqa
from kombu.utils.uuid import uuid   # noqa
gen_unique_id = uuid
