# -*- coding: utf-8 -*-
"""
=============
Dynamic Tasks
=============

Subtasks returned by dynamic tasks are executed right after the first
task executes. As if they were in a chain.
You can also return chains and chords, and they will be properly inserted.

This allows you to design a pipeline that can be completely dynamic, while
benefiting from Celery's powerful idioms (subtasks, chains, chords...).


Usage example
-------------

.. code-block:: python

    from celery import task
    from celery.contrib.dynamic import dynamic_task

    @task
    @dynamic_task
    def one(i):
        if i > 3:
            return two.s(i)
        return i + 1

    @task
    @dynamic_task
    def two(i):
        return i + 2

    def test():
        one.delay(2)  # will run: one.s(2) => 3
        one.delay(5)  # will run: one.s(5) | two.s() => 7

"""
from __future__ import absolute_import

from celery import current_task
from celery.canvas import Signature
from functools import wraps


def dynamic_task(fn):
    @wraps(fn)
    def _wrap(*args, **kwargs):
        retval = fn(*args, **kwargs)
        if isinstance(retval, Signature):
            retval.immutable = True
            if current_task.request.callbacks:
                retval |= current_task.request.callbacks[0]
            if current_task.request.chord:
                retval.set(chord=current_task.request.chord)
                current_task.request.chord = None
            if current_task.request.group:
                retval.set(group_id=current_task.request.group)
                current_task.request.group = None
            current_task.request.callbacks = [retval]
        return retval
    return _wrap
