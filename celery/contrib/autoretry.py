# -*- coding: utf-8 -*-
"""
celery.contrib.autoretry
========================

.. versionadded:: 3.2

Decorator that enables autoretrying when one of specified exceptions
are raised in a task body.


Examples
--------

.. code-block:: python

    from celery.contrib.autoretry import autoretry

    @autoretry(on=(ZeroDivisionError,))
    @app.task
    def div

.. note::

    `autoretry` decorator must be applied **before** `app.task` decorator.
"""

from __future__ import absolute_import

from functools import wraps


def autoretry(on=None, retry_kwargs=None):

    def decorator(task):
        if not on:
            return task.run

        autoretry_exceptions = tuple(on)  # except only works with tuples
        _retry_kwargs = retry_kwargs or {}

        @wraps(task.run)
        def inner(*args, **kwargs):
            try:
                return task._orig_run(*args, **kwargs)
            except autoretry_exceptions as exc:
                raise task.retry(exc=exc, **_retry_kwargs)

        task._orig_run = task.run
        task.run = inner
        return inner
    return decorator
