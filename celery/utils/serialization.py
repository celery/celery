# -*- coding: utf-8 -*-
"""
    celery.utils.serialization
    ~~~~~~~~~~~~~~~~~~~~~~~~~~

    Utilities for safely pickling exceptions.

"""
from __future__ import absolute_import

import inspect
import sys
import types

import pickle as pypickle
try:
    import cPickle as cpickle
except ImportError:
    cpickle = None  # noqa

from .encoding import safe_repr


if sys.version_info < (2, 6):  # pragma: no cover
    # cPickle is broken in Python <= 2.6.
    # It unsafely and incorrectly uses relative instead of absolute imports,
    # so e.g.:
    #       exceptions.KeyError
    # becomes:
    #       celery.exceptions.KeyError
    #
    # Your best choice is to upgrade to Python 2.6,
    # as while the pure pickle version has worse performance,
    # it is the only safe option for older Python versions.
    pickle = pypickle
else:
    pickle = cpickle or pypickle

#: List of base classes we probably don't want to reduce to.
unwanted_base_classes = (StandardError, Exception, BaseException, object)

if sys.version_info < (2, 5):  # pragma: no cover

    # Prior to Python 2.5, Exception was an old-style class
    def subclass_exception(name, parent, unused):
        return types.ClassType(name, (parent,), {})
else:

    def subclass_exception(name, parent, module):  # noqa
        return type(name, (parent,), {'__module__': module})


def find_nearest_pickleable_exception(exc):
    """With an exception instance, iterate over its super classes (by mro)
    and find the first super exception that is pickleable.  It does
    not go below :exc:`Exception` (i.e. it skips :exc:`Exception`,
    :class:`BaseException` and :class:`object`).  If that happens
    you should use :exc:`UnpickleableException` instead.

    :param exc: An exception instance.

    :returns: the nearest exception if it's not :exc:`Exception` or below,
              if it is it returns :const:`None`.

    :rtype :exc:`Exception`:

    """
    cls = exc.__class__
    getmro_ = getattr(cls, 'mro', None)

    # old-style classes doesn't have mro()
    if not getmro_:  # pragma: no cover
        # all Py2.4 exceptions has a baseclass.
        if not getattr(cls, '__bases__', ()):
            return
        # Use inspect.getmro() to traverse bases instead.
        getmro_ = lambda: inspect.getmro(cls)

    for supercls in getmro_():
        if supercls in unwanted_base_classes:
            # only BaseException and object, from here on down,
            # we don't care about these.
            return
        try:
            exc_args = getattr(exc, 'args', [])
            superexc = supercls(*exc_args)
            pickle.loads(pickle.dumps(superexc))
        except:
            pass
        else:
            return superexc


def create_exception_cls(name, module, parent=None):
    """Dynamically create an exception class."""
    if not parent:
        parent = Exception
    return subclass_exception(name, parent, module)


class UnpickleableExceptionWrapper(Exception):
    """Wraps unpickleable exceptions.

    :param exc_module: see :attr:`exc_module`.
    :param exc_cls_name: see :attr:`exc_cls_name`.
    :param exc_args: see :attr:`exc_args`

    **Example**

    .. code-block:: python

        >>> try:
        ...     something_raising_unpickleable_exc()
        >>> except Exception, e:
        ...     exc = UnpickleableException(e.__class__.__module__,
        ...                                 e.__class__.__name__,
        ...                                 e.args)
        ...     pickle.dumps(exc) # Works fine.

    """

    #: The module of the original exception.
    exc_module = None

    #: The name of the original exception class.
    exc_cls_name = None

    #: The arguments for the original exception.
    exc_args = None

    def __init__(self, exc_module, exc_cls_name, exc_args, text=None):
        safe_exc_args = []
        for arg in exc_args:
            try:
                pickle.dumps(arg)
                safe_exc_args.append(arg)
            except Exception:
                safe_exc_args.append(safe_repr(arg))
        self.exc_module = exc_module
        self.exc_cls_name = exc_cls_name
        self.exc_args = safe_exc_args
        self.text = text
        Exception.__init__(self, exc_module, exc_cls_name, safe_exc_args, text)

    def restore(self):
        return create_exception_cls(self.exc_cls_name,
                                    self.exc_module)(*self.exc_args)

    def __str__(self):
        return self.text

    @classmethod
    def from_exception(cls, exc):
        return cls(exc.__class__.__module__,
                   exc.__class__.__name__,
                   getattr(exc, 'args', []),
                   safe_repr(exc))


def get_pickleable_exception(exc):
    """Make sure exception is pickleable."""
    try:
        pickle.loads(pickle.dumps(exc))
    except Exception:
        pass
    else:
        return exc
    nearest = find_nearest_pickleable_exception(exc)
    if nearest:
        return nearest
    return UnpickleableExceptionWrapper.from_exception(exc)


def get_pickled_exception(exc):
    """Get original exception from exception pickled using
    :meth:`get_pickleable_exception`."""
    if isinstance(exc, UnpickleableExceptionWrapper):
        return exc.restore()
    return exc
