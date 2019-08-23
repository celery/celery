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
task decorated objects (e.g. when using the automodule directive)
and generate the correct (as well as add a ``(task)`` prefix),
and you can also refer to the tasks using `:task:proj.tasks.add`
syntax.

Use ``.. autotask::`` to alternatively manually document a task.
"""
from __future__ import absolute_import, unicode_literals

from celery.app.task import BaseTask
from sphinx.domains.python import PyModulelevel
from sphinx.ext.autodoc import FunctionDocumenter

try:  # pragma: no cover
    from inspect import formatargspec, getfullargspec
except ImportError:  # Py2
    from inspect import formatargspec, getargspec as getfullargspec  # noqa


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
            if argspec[0] and argspec[0][0] in ('cls', 'self'):
                del argspec[0][0]
            fmt = formatargspec(*argspec)
            fmt = fmt.replace('\\', '\\\\')
            return fmt
        return ''

    def document_members(self, all_members=False):
        pass

    def check_module(self):
        # Normally checks if *self.object* is really defined in the module
        # given by *self.modname*. But since functions decorated with the @task
        # decorator are instances living in the celery.local, we have to check
        # the wrapped function instead.
        wrapped = getattr(self.object, '__wrapped__', None)
        if wrapped and getattr(wrapped, '__module__') == self.modname:
            return True
        return super(TaskDocumenter, self).check_module()


class TaskDirective(PyModulelevel):
    """Sphinx task directive."""

    def get_signature_prefix(self, sig):
        return self.env.config.celery_task_prefix


def autodoc_skip_member_handler(app, what, name, obj, skip, options):
    """Handler for autodoc-skip-member event."""
    # Celery tasks created with the @task decorator have the property
    # that *obj.__doc__* and *obj.__class__.__doc__* are equal, which
    # trips up the logic in sphinx.ext.autodoc that is supposed to
    # suppress repetition of class documentation in an instance of the
    # class. This overrides that behavior.
    if isinstance(obj, BaseTask) and getattr(obj, '__wrapped__'):
        if skip:
            return False
    return None


def setup(app):
    """Setup Sphinx extension."""
    app.setup_extension('sphinx.ext.autodoc')
    app.add_autodocumenter(TaskDocumenter)
    app.add_directive_to_domain('py', 'task', TaskDirective)
    app.add_config_value('celery_task_prefix', '(task)', True)
    app.connect('autodoc-skip-member', autodoc_skip_member_handler)

    return {
        'parallel_read_safe': True
    }
