# -*- coding: utf-8 -*-
"""Utility functions.

Don't import from here directly anymore, as these are only
here for backwards compatibility.
"""
from __future__ import absolute_import, print_function, unicode_literals

from kombu.utils.objects import cached_property  # noqa: F401
from kombu.utils.uuid import uuid  # noqa: F401

from .functional import memoize  # noqa: F401
from .functional import chunks, noop  # noqa: F401
from .imports import gen_task_name, import_from_cwd, instantiate  # noqa: F401
from .imports import qualname as get_full_cls_name  # noqa: F401
from .imports import symbol_by_name as get_cls_by_name  # noqa: F401
# ------------------------------------------------------------------------ #
# > XXX Compat
from .log import LOG_LEVELS  # noqa
from .nodenames import nodename, nodesplit, worker_direct

__all__ = ('worker_direct', 'gen_task_name', 'nodename', 'nodesplit',
           'cached_property', 'uuid')

gen_unique_id = uuid
