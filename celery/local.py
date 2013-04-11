# -*- coding: utf-8 -*-
"""
    celery.local
    ~~~~~~~~~~~~

    This module contains critical utilities that
    needs to be loaded as soon as possible, and that
    shall not load any third party modules.

    Parts of this module is Copyright by Werkzeug Team.

"""
from __future__ import absolute_import

import importlib
import sys

PY3 = sys.version_info[0] == 3

__module__ = __name__  # used by Proxy class body


def _default_cls_attr(name, type_, cls_value):
    # Proxy uses properties to forward the standard
    # class attributes __module__, __name__ and __doc__ to the real
    # object, but these needs to be a string when accessed from
    # the Proxy class directly.  This is a hack to make that work.
    # -- See Issue #1087.

    def __new__(cls, getter):
        instance = type_.__new__(cls, cls_value)
        instance.__getter = getter
        return instance

    def __get__(self, obj, cls=None):
        return self.__getter(obj) if obj is not None else self

    def __set__(self, obj, value):
        raise AttributeError('readonly attribute')

    return type(name, (type_, ), {
        '__new__': __new__, '__get__': __get__, '__set__': __set__,
    })


class _cls_spec(str):

    def __new__(cls, getter):
        s = str.__new__(cls, getter.__module__)
        s.__getter = getter
        return s

    def __get__(self, obj, cls=None):
        if obj is not None:
            return self.__getter(obj)
        return self

    def __set__(self, obj, value):
        raise AttributeError('cannot set attribute')


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

        >>> symbol_by_name('celery.concurrency.processes.TaskPool')
        <class 'celery.concurrency.processes.TaskPool'>

        >>> symbol_by_name('default', {
        ...     'default': 'celery.concurrency.processes.TaskPool'})
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
    if not module_name:
        cls_name, module_name = None, package if package else cls_name
    try:
        try:
            module = imp(module_name, package=package, **kwargs)
        except ValueError, exc:
            raise ValueError, ValueError(
                "Couldn't import %r: %s" % (name, exc)), sys.exc_info()[2]
        return getattr(module, cls_name) if cls_name else module
    except (ImportError, AttributeError):
        if default is None:
            raise
    return default


def try_import(module, default=None):
    """Try to import and return module, or return
    None if the module does not exist."""
    try:
        return importlib.import_module(module)
    except ImportError:
        return default


class Proxy(object):
    """Proxy to another object."""

    # Code stolen from werkzeug.local.Proxy.
    __slots__ = ('__local', '__args', '__kwargs', '__dict__')
    if not PY3:
        __slots__ += ('__name__', )

    def __init__(self, local, args=None, kwargs=None, name=None):
        object.__setattr__(self, '_Proxy__local', local)
        object.__setattr__(self, '_Proxy__args', args or ())
        object.__setattr__(self, '_Proxy__kwargs', kwargs or {})
        if name is not None:
            object.__setattr__(self, '__custom_name__', name)

    @_default_cls_attr('name', str, __name__)
    def __name__(self):
        try:
            return self.__custom_name__
        except AttributeError:
            return self._get_current_object().__name__

    @_default_cls_attr('module', str, __module__)
    def __module__(self):
        return self._get_current_object().__module__

    @_default_cls_attr('doc', str, __doc__)
    def __doc__(self):
        return self._get_current_object().__doc__

    def _get_class(self):
        return self._get_current_object().__class__

    @property
    def __class__(self):
        return self._get_class()

    def _get_current_object(self):
        """Return the current object.  This is useful if you want the real
        object behind the proxy at a time for performance reasons or because
        you want to pass the object into a different context.
        """
        loc = object.__getattribute__(self, '_Proxy__local')
        if not hasattr(loc, '__release_local__'):
            return loc(*self.__args, **self.__kwargs)
        try:
            return getattr(loc, self.__name__)
        except AttributeError:
            raise RuntimeError('no object bound to %s' % self.__name__)

    @property
    def __dict__(self):
        try:
            return self._get_current_object().__dict__
        except RuntimeError:  # pragma: no cover
            raise AttributeError('__dict__')

    def __repr__(self):
        try:
            obj = self._get_current_object()
        except RuntimeError:  # pragma: no cover
            return '<%s unbound>' % self.__class__.__name__
        return repr(obj)

    def __nonzero__(self):
        try:
            return bool(self._get_current_object())
        except RuntimeError:  # pragma: no cover
            return False

    def __unicode__(self):
        try:
            return unicode(self._get_current_object())
        except RuntimeError:  # pragma: no cover
            return repr(self)

    def __dir__(self):
        try:
            return dir(self._get_current_object())
        except RuntimeError:  # pragma: no cover
            return []

    def __getattr__(self, name):
        if name == '__members__':
            return dir(self._get_current_object())
        return getattr(self._get_current_object(), name)

    def __setitem__(self, key, value):
        self._get_current_object()[key] = value

    def __delitem__(self, key):
        del self._get_current_object()[key]

    def __setslice__(self, i, j, seq):
        self._get_current_object()[i:j] = seq

    def __delslice__(self, i, j):
        del self._get_current_object()[i:j]

    __setattr__ = lambda x, n, v: setattr(x._get_current_object(), n, v)
    __delattr__ = lambda x, n: delattr(x._get_current_object(), n)
    __str__ = lambda x: str(x._get_current_object())
    __lt__ = lambda x, o: x._get_current_object() < o
    __le__ = lambda x, o: x._get_current_object() <= o
    __eq__ = lambda x, o: x._get_current_object() == o
    __ne__ = lambda x, o: x._get_current_object() != o
    __gt__ = lambda x, o: x._get_current_object() > o
    __ge__ = lambda x, o: x._get_current_object() >= o
    __cmp__ = lambda x, o: cmp(x._get_current_object(), o)
    __hash__ = lambda x: hash(x._get_current_object())
    __call__ = lambda x, *a, **kw: x._get_current_object()(*a, **kw)
    __len__ = lambda x: len(x._get_current_object())
    __getitem__ = lambda x, i: x._get_current_object()[i]
    __iter__ = lambda x: iter(x._get_current_object())
    __contains__ = lambda x, i: i in x._get_current_object()
    __getslice__ = lambda x, i, j: x._get_current_object()[i:j]
    __add__ = lambda x, o: x._get_current_object() + o
    __sub__ = lambda x, o: x._get_current_object() - o
    __mul__ = lambda x, o: x._get_current_object() * o
    __floordiv__ = lambda x, o: x._get_current_object() // o
    __mod__ = lambda x, o: x._get_current_object() % o
    __divmod__ = lambda x, o: x._get_current_object().__divmod__(o)
    __pow__ = lambda x, o: x._get_current_object() ** o
    __lshift__ = lambda x, o: x._get_current_object() << o
    __rshift__ = lambda x, o: x._get_current_object() >> o
    __and__ = lambda x, o: x._get_current_object() & o
    __xor__ = lambda x, o: x._get_current_object() ^ o
    __or__ = lambda x, o: x._get_current_object() | o
    __div__ = lambda x, o: x._get_current_object().__div__(o)
    __truediv__ = lambda x, o: x._get_current_object().__truediv__(o)
    __neg__ = lambda x: -(x._get_current_object())
    __pos__ = lambda x: +(x._get_current_object())
    __abs__ = lambda x: abs(x._get_current_object())
    __invert__ = lambda x: ~(x._get_current_object())
    __complex__ = lambda x: complex(x._get_current_object())
    __int__ = lambda x: int(x._get_current_object())
    __long__ = lambda x: long(x._get_current_object())
    __float__ = lambda x: float(x._get_current_object())
    __oct__ = lambda x: oct(x._get_current_object())
    __hex__ = lambda x: hex(x._get_current_object())
    __index__ = lambda x: x._get_current_object().__index__()
    __coerce__ = lambda x, o: x.__coerce__(x, o)
    __enter__ = lambda x: x._get_current_object().__enter__()
    __exit__ = lambda x, *a, **kw: x._get_current_object().__exit__(*a, **kw)
    __reduce__ = lambda x: x._get_current_object().__reduce__()


class PromiseProxy(Proxy):
    """This is a proxy to an object that has not yet been evaulated.

    :class:`Proxy` will evaluate the object each time, while the
    promise will only evaluate it once.

    """

    def _get_current_object(self):
        try:
            return object.__getattribute__(self, '__thing')
        except AttributeError:
            return self.__evaluate__()

    def __evaluated__(self):
        try:
            object.__getattribute__(self, '__thing')
        except AttributeError:
            return False
        return True

    def __maybe_evaluate__(self):
        return self._get_current_object()

    def __evaluate__(self):
        try:
            thing = Proxy._get_current_object(self)
            object.__setattr__(self, '__thing', thing)
            return thing
        finally:
            object.__delattr__(self, '_Proxy__local')
            object.__delattr__(self, '_Proxy__args')
            object.__delattr__(self, '_Proxy__kwargs')


def maybe_evaluate(obj):
    try:
        return obj.__maybe_evaluate__()
    except AttributeError:
        return obj
