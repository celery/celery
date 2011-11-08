# -*- coding: utf-8 -*-
"""
celery.decorators✞
==================

Deprecated decorators, use `celery.task.task`,
and `celery.task.periodic_task` instead.

The new decorators does not support magic keyword arguments.

:copyright: (c) 2009 - 2011 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import warnings

from . import task as _task
from .exceptions import CDeprecationWarning


warnings.warn(CDeprecationWarning("""
The `celery.decorators` module and the magic keyword arguments
are pending deprecation and will be deprecated in 2.4, then removed
in 3.0.

`task.request` should be used instead of magic keyword arguments,
and `celery.task.task` used instead of `celery.decorators.task`.

See the 2.2 Changelog for more information.

"""))


def task(*args, **kwargs):  # ✞
    kwargs.setdefault("accept_magic_kwargs", True)
    return _task.task(*args, **kwargs)


def periodic_task(*args, **kwargs):  # ✞
    kwargs.setdefault("accept_magic_kwargs", True)
    return _task.periodic_task(*args, **kwargs)
