from __future__ import absolute_import
from __future__ import with_statement

import imp as _imp
import importlib
import os
import sys

from contextlib import contextmanager

from .compat import reload


class NotAPackage(Exception):
    pass


if sys.version_info >= (3, 3):

    def qualname(obj):
        return obj.__qualname__

else:

    def qualname(obj):  # noqa
        if not hasattr(obj, "__name__") and hasattr(obj, "__class__"):
            return qualname(obj.__class__)

        return '.'.join([obj.__module__, obj.__name__])


def symbol_by_name(name, aliases={}, imp=None, package=None,
        sep='.', default=None, **kwargs):
    """Get symbol by qualified name.

    The name should be the full dot-separated path to the class::

        modulename.ClassName

    Example::

        celery.concurrency.processes.TaskPool
                                    ^- class name

    or using ':' to separate module and symbol::

        celery.concurrency.processes:TaskPool

    If `aliases` is provided, a dict containing short name/long name
    mappings, the name is looked up in the aliases first.

    Examples:

        >>> symbol_by_name("celery.concurrency.processes.TaskPool")
        <class 'celery.concurrency.processes.TaskPool'>

        >>> symbol_by_name("default", {
        ...     "default": "celery.concurrency.processes.TaskPool"})
        <class 'celery.concurrency.processes.TaskPool'>

        # Does not try to look up non-string names.
        >>> from celery.concurrency.processes import TaskPool
        >>> symbol_by_name(TaskPool) is TaskPool
        True

    """
    if imp is None:
        imp = importlib.import_module

    if not isinstance(name, basestring):
        return name                                 # already a class

    name = aliases.get(name) or name
    sep = ':' if ':' in name else sep
    module_name, _, cls_name = name.rpartition(sep)
    if not module_name and package:
        module_name = package
    try:
        try:
            module = imp(module_name, package=package, **kwargs)
        except ValueError, exc:
            raise ValueError, ValueError(
                    "Couldn't import %r: %s" % (name, exc)), sys.exc_info()[2]
        return getattr(module, cls_name)
    except (ImportError, AttributeError):
        if default is None:
            raise
    return default


def instantiate(name, *args, **kwargs):
    """Instantiate class by name.

    See :func:`symbol_by_name`.

    """
    return symbol_by_name(name)(*args, **kwargs)


@contextmanager
def cwd_in_path():
    cwd = os.getcwd()
    if cwd in sys.path:
        yield
    else:
        sys.path.insert(0, cwd)
        try:
            yield cwd
        finally:
            try:
                sys.path.remove(cwd)
            except ValueError:
                pass


def find_module(module, path=None, imp=None):
    """Version of :func:`imp.find_module` supporting dots."""
    if imp is None:
        imp = importlib.import_module
    with cwd_in_path():
        if "." in module:
            last = None
            parts = module.split(".")
            for i, part in enumerate(parts[:-1]):
                mpart = imp(".".join(parts[:i + 1]))
                try:
                    path = mpart.__path__
                except AttributeError:
                    raise NotAPackage(module)
                last = _imp.find_module(parts[i + 1], path)
            return last
        return _imp.find_module(module)


def import_from_cwd(module, imp=None, package=None):
    """Import module, but make sure it finds modules
    located in the current directory.

    Modules located in the current directory has
    precedence over modules located in `sys.path`.
    """
    if imp is None:
        imp = importlib.import_module
    with cwd_in_path():
        return imp(module, package=package)


def reload_from_cwd(module, reloader=None):
    if reloader is None:
        reloader = reload
    with cwd_in_path():
        return reloader(module)
