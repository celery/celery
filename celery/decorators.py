# -*- coding: utf-8 -*-
"""
celery.decorators✞
==================

Depreacted decorators, use `celery.task.task`,
and `celery.task.periodic_task` instead.

The new decorators does not support magic keyword arguments.

:copyright: (c) 2009 - 2010 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
import warnings

from celery import task as _task


DEPRECATION_TEXT = """\
The `celery.decorators` module and the magic keyword arguments
are pending deprecation and will be deprecated in 2.4, then removed
in 3.0.

`task.request` should be used instead of magic keyword arguments,
and `celery.task.task` used instead of `celery.decorators.task`.

"""


def task(*args, **kwargs):  # ✞
    warnings.warn(PendingDeprecationWarning(DEPRECATION_TEXT))
    kwargs.setdefault("accept_magic_kwargs", True)
    return _task.task(*args, **kwargs)


def periodic_task(*args, **kwargs):  # ✞
    warnings.warn(PendingDeprecationWarning(DEPRECATION_TEXT))
    kwargs.setdefault("accept_magic_kwargs", True)
    return _task.periodic_task(*args, **kwargs)
