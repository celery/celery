# -*- coding: utf-8 -*-
"""
=============
Dynamic Tasks
=============

Subtasks returned by dynamic tasks are executed right after the first
task executes. As if they were in a chain.
You can also return chains and chords, and they will be properly inserted.
You can have chords in chords etc...

This allows you to design a pipeline that can be completely dynamic, while
benefiting from Celery's powerful idioms (subtasks, chains, chords...).


Usage example
-------------

.. code-block:: python

    import celery.contrib.dynamic
    from celery.contrib.dynamic import dynamic_chord as chord

    @app.dynamic_task
    def one(i):
        if i > 3:
            return two.s(i)
        return i + 1

    @app.dynamic_task(ignore_result=True)
    def two(i):
        return i + 2

    def test():
        one.delay(2)  # will run: one.s(2) => 3
        one.delay(5)  # will run: one.s(5) | two.s() => 7

"""
from __future__ import absolute_import

from celery import current_task, Celery, subtask, chord
from celery.app.builtins import shared_task
from celery.canvas import Signature
from functools import wraps


def dynamic_task(self, *args, **kwargs):
    def do_wrap(fn):
        @wraps(fn)
        def _fn_wrapper(*args, **kwargs):
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
        return self.task(*args, **kwargs)(_fn_wrapper)
    if len(args) == 1 and callable(args[0]):
        fn = args[0]
        args = args[1:]
        return do_wrap(fn)
    return do_wrap
setattr(Celery, "dynamic_task", dynamic_task)


@shared_task
def add_checkpoint_task(app):
    @app.task(name='celery.checkpoint')
    def checkpoint(result):
        return result
    return checkpoint


def dynamic_chord(tasks, callback):
    return chord([(task | subtask("celery.checkpoint")) for task in tasks], callback)
