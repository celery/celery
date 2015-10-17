# -*- coding: utf-8 -*-
"""
    celery.utils.import
    ~~~~~~~~~~~~~~~~~~~

    Utilities related to importing modules and symbols by name.

"""
from __future__ import absolute_import

import imp as _imp
import importlib
import os
import sys
from copy import deepcopy
from inspect import ismodule

from contextlib import contextmanager

from distutils.sysconfig import get_python_lib

from kombu.utils import symbol_by_name

from celery.five import reload, builtins, string_t

__all__ = [
    'NotAPackage', 'qualname', 'instantiate', 'symbol_by_name', 'cwd_in_path',
    'find_module', 'import_from_cwd', 'reload_from_cwd', 'module_file',
    'DependencyTracker',
]


class NotAPackage(Exception):
    pass


if sys.version_info > (3, 3):  # pragma: no cover
    def qualname(obj):
        if not hasattr(obj, '__name__') and hasattr(obj, '__class__'):
            obj = obj.__class__
        q = getattr(obj, '__qualname__', None)
        if '.' not in q:
            q = '.'.join((obj.__module__, q))
        return q
else:
    def qualname(obj):  # noqa
        if not hasattr(obj, '__name__') and hasattr(obj, '__class__'):
            obj = obj.__class__
        return '.'.join((obj.__module__, obj.__name__))


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
            except ValueError:  # pragma: no cover
                pass


def find_module(module, path=None, imp=None):
    """Version of :func:`imp.find_module` supporting dots."""
    if imp is None:
        imp = importlib.import_module
    with cwd_in_path():
        if '.' in module:
            last = None
            parts = module.split('.')
            for i, part in enumerate(parts[:-1]):
                mpart = imp('.'.join(parts[:i + 1]))
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


def module_file(module):
    """Return the correct original file name of a module."""
    name = module.__file__
    return name[:-1] if name.endswith('.pyc') else name


class DependencyTracker(object):
    """Tracks for module dependencies and allows to reload modules fairly
    (reload all module dependencies recursively with correct ordering).

    Originally taken from
    https://github.com/jparise/python-reloader/blob/master/reloader.py
    """

    def __init__(self, blacklist=None):
        # We don't want to reload stdlib modules by default
        _blacklist = std_modules()
        # Also, celery itself shouldn't be reloaded.
        # Actually, it can't.
        _blacklist.add('celery')

        if blacklist:
            if isinstance(blacklist, string_t):
                blacklist = (blacklist,)
            _blacklist.update(set(blacklist))
        self.blacklist = _blacklist

        self._orig_import = builtins.__import__
        self._dependencies = {}
        self._parent = None

        # PEP 328 changed the default level to 0 in Python 3.3.
        self._default_level = -1 if sys.version_info < (3, 3) else 0

        self._frozen = False

    def enable(self):
        """Enable global module dependency tracking."""
        builtins.__import__ = self._import
        self._frozen = False

    def freeze(self):
        """Disable global module dependency tracking, but freeze
        already collected dependencies.
        """
        self._frozen = True

    def disable(self):
        """Disable global module dependency tracking."""
        builtins.__import__ = self._orig_import
        self._parent = None
        self._dependencies.clear()
        self._frozen = False

    def get_dependencies(self, module):
        """Get the dependency list for the given imported module."""
        name = module.__name__ if ismodule(module) else module
        return self._dependencies.get(name)

    def _reload(self, module, visited, reloader):
        """Internal module reloading routine."""
        name = module.__name__

        # If this module's name appears in our blacklist, skip its entire
        # dependency hierarchy.
        if name in self.blacklist:
            return

        # Start by adding this module to our set of visited modules.
        # We use this set to avoid running into infinite recursion
        # while walking the module dependency graph.
        visited.add(module)

        # Start by reloading all of our dependencies in reverse order.
        # Note that we recursively call ourself to perform the nested reloads.
        deps = self._dependencies.get(name)
        if deps is not None:
            for dep in reversed(deps):
                if dep not in visited:
                    self._reload(dep, visited, reloader)

        # Clear this module's list of dependencies. Some import statements may
        # have been removed. We'll rebuild the dependency list as part of the
        # reload operation below.
        try:
            del self._dependencies[name]
        except KeyError:
            pass

        # Because we're triggering a reload and not an import, the module
        # itself won't run through our _import hook below. In order for this
        # module's dependencies (which will pass through the _import hook)
        # to be associated with this module, we need to set our parent pointer
        # beforehand.
        self._parent = name

        # If the module has a __reload__(d) function, we'll call it with a copy
        # of the original module's dictionary after it's been reloaded.
        callback = getattr(module, '__reload__', None)
        if callback is not None:
            d = _deepcopy_module_dict(module)
            reloader(module)
            callback(d)
        else:
            reloader(module)

        # Reset our parent pointer now that the reloading operation is complete
        self._parent = None

    def reload(self, module, reloader=None):
        """Reload an existing module.

        Any known dependencies of the module will also be reloaded.

        If a module has a __reload__(d) function, it will be called with a copy
        of the original module's dictionary after the module is reloaded.
        """
        if reloader is None:
            reloader = reload
        self._reload(module, set(), reloader)

    def _import(self, name, globals=None, locals=None,
                fromlist=None, level=None):
        """__import__() replacement function that tracks module
        dependencies.
        """
        level = level or self._default_level
        # Track our current parent module. This is used to find our current
        # place in the dependency graph.
        parent = self._parent
        self._parent = name

        # Perform the actual import work using the base import function.
        base = self._orig_import(name, globals, locals, fromlist, level)

        if base is not None and parent is not None:
            module = base

            # We manually walk through the imported hierarchy because
            # the import function only returns the top-level package reference
            # for a nested import statement
            # (e.g. 'package' for `import package.module`) when
            # no fromlist has been specified.
            if fromlist is None:
                for component in name.split('.')[1:]:
                    module = getattr(module, component)

            # If this is a nested import for a reloadable (source-based)
            # module, we append ourself to our parent's dependency list.
            if hasattr(module, '__file__'):
                try:
                    self._dependencies[parent].append(module)
                except KeyError:
                    if not self._frozen:
                        self._dependencies[parent] = [module]

        # Lastly, we always restore our _parent pointer.
        self._parent = parent

        return base


def _deepcopy_module_dict(module):
    """Make a deep copy of a module's dictionary."""

    # We can't deepcopy() everything in the module's dictionary
    # because some items, such as '__builtins__', aren't deepcopy()-able.
    # To work around that, we start by making a shallow copy
    # of the dictionary, giving us a way to remove keys
    # before performing the deep copy.
    d = module.__dict__.copy()
    del d['__builtins__']
    return deepcopy(d)


def std_modules():
    """Returns set of names of standard modules. Some of modules (like os.path)
    may not be listed.
    Originally taken from http://stackoverflow.com/a/8992937
    """
    sep = os.path.sep
    # We need this, because Debian uses dist-packages directory
    # instead of site-packages
    site_packages = get_python_lib(standard_lib=False).split(sep)[-1]
    std_lib = get_python_lib(standard_lib=True)

    std_modules = set()
    for top, dirs, files in os.walk(std_lib):
        for name in files:
            prefix = top[len(std_lib)+1:]
            if prefix[:13] == site_packages:
                continue
            if name == '__init__.py':
                std_modules.add(top[len(std_lib)+1:].replace(
                                sep, '.'))
            elif name[-3:] == '.py':
                std_modules.add(os.path.join(prefix, name)[:-3].replace(
                                sep, '.'))
            elif name[-3:] == '.so' and top[-11:] == 'lib-dynload':
                std_modules.add(name[0:-3])
    return std_modules
