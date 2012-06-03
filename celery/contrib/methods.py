"""
celery.contrib.methods
======================

Task decorator that supports creating tasks out of methods.

Examples
--------

.. code-block:: python

    from celery.contrib.methods import task

    class X(object):

        @task
        def add(self, x, y):
                return x + y

or with any task decorator:

.. code-block:: python

    from celery.contrib.methods import task_method

    class X(object):

        @celery.task(filter=task_method)
        def add(self, x, y):
            return x + y

Caveats
-------

- Automatic naming won't be able to know what the class name is.

    The name will still be module_name + task_name,
    so two methods with the same name in the same module will collide
    so that only one task can run:

    .. code-block:: python

        class A(object):

            @task
            def add(self, x, y):
                return x + y

        class B(object):

            @task
            def add(self, x, y):
                return x + y

    would have to be written as:

    .. code-block:: python

        class A(object):
            @task(name="A.add")
            def add(self, x, y):
                return x + y

        class B(object):
            @task(name="B.add")
            def add(self, x, y):
                return x + y

"""

from __future__ import absolute_import

from functools import partial

from celery import task as _task


class task_method(object):

    def __init__(self, task, *args, **kwargs):
        self.task = task

    def __get__(self, obj, type=None):
        if obj is None:
            return self.task
        task = self.task.__class__()
        task.__self__ = obj
        return task


task = partial(_task, filter=task_method)
