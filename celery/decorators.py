# -*- coding: utf-8 -*-
"""
celery.decorators✞
==================

Deprecated decorators, use `celery.task.task`,
and `celery.task.periodic_task` instead.

The new decorators does not support magic keyword arguments.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import warnings

from . import task as _task
from .exceptions import CDeprecationWarning


warnings.warn(CDeprecationWarning("""
The `celery.decorators` module along with the magic keyword arguments,
are deprecated, and will be removed in version 3.0.

Please use the `celery.task` module instead of `celery.decorators`,
and the `task.request` should be used instead of the magic keyword arguments:

    from celery.task import task

See http://bit.ly/celery22major for more information.

"""))


def task(*args, **kwargs):  # ✞
    kwargs.setdefault("accept_magic_kwargs", True)
    return _task.task(*args, **kwargs)


def periodic_task(*args, **kwargs):  # ✞
    kwargs.setdefault("accept_magic_kwargs", True)
    return _task.periodic_task(*args, **kwargs)
