# -*- coding: utf-8 -*-
"""Sphinx documentation plugin used to document tasks.

Introduction
============

Usage
-----

Add the extension to your :file:`docs/conf.py` configuration module:

.. code-block:: python

    extensions = (...,
                  'celery.contrib.sphinx')

If you'd like to change the prefix for tasks in reference documentation
then you can change the ``celery_task_prefix`` configuration value:

.. code-block:: python

    celery_task_prefix = '(task)'  # < default

With the extension installed `autodoc` will automatically find
task decorated objects and generate the correct (as well as
add a ``(task)`` prefix), and you can also refer to the tasks
using `:task:proj.tasks.add` syntax.

Use ``.. autotask::`` to manually document a task.
"""
from __future__ import absolute_import, unicode_literals
from inspect import formatargspec
from sphinx.domains.python import PyModulelevel
from sphinx.ext.autodoc import FunctionDocumenter
from celery.app.task import BaseTask
from celery.five import getfullargspec


class TaskDocumenter(FunctionDocumenter):
    """Document task definitions."""

    objtype = 'task'
    member_order = 11

    @classmethod
    def can_document_member(cls, member, membername, isattr, parent):
        return isinstance(member, BaseTask) and getattr(member, '__wrapped__')

    def format_args(self):
        wrapped = getattr(self.object, '__wrapped__', None)
        if wrapped is not None:
            argspec = getfullargspec(wrapped)
            fmt = formatargspec(*argspec)
            fmt = fmt.replace('\\', '\\\\')
            return fmt
        return ''

    def document_members(self, all_members=False):
        pass


class TaskDirective(PyModulelevel):
    """Sphinx task directive."""

    def get_signature_prefix(self, sig):
        return self.env.config.celery_task_prefix


def setup(app):
    """Setup Sphinx extension."""
    app.add_autodocumenter(TaskDocumenter)
    app.add_directive_to_domain('py', 'task', TaskDirective)
    app.add_config_value('celery_task_prefix', '(task)', True)
