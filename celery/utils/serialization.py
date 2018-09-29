# -*- coding: utf-8 -*-
"""Utilities for safely pickling exceptions."""
from __future__ import absolute_import, unicode_literals

import datetime
import numbers
import sys
from base64 import b64decode as base64decode
from base64 import b64encode as base64encode
from functools import partial
from inspect import getmro
from itertools import takewhile

from kombu.utils.encoding import bytes_to_str, str_to_bytes

from celery.five import (bytes_if_py2, items, python_2_unicode_compatible,
                         reraise, string_t)

from .encoding import safe_repr

try:
    import cPickle as pickle
except ImportError:
    import pickle  # noqa

PY33 = sys.version_info >= (3, 3)

__all__ = (
    'UnpickleableExceptionWrapper', 'subclass_exception',
    'find_pickleable_exception', 'create_exception_cls',
    'get_pickleable_exception', 'get_pickleable_etype',
    'get_pickled_exception', 'strtobool',
)

#: List of base classes we probably don't want to reduce to.
try:
    unwanted_base_classes = (StandardError, Exception, BaseException, object)
except NameError:  # pragma: no cover
    unwanted_base_classes = (Exception, BaseException, object)  # py3k


def subclass_exception(name, parent, module):  # noqa
    """Create new exception class."""
    return type(bytes_if_py2(name), (parent,), {'__module__': module})


def find_pickleable_exception(exc, loads=pickle.loads,
                              dumps=pickle.dumps):
    """Find first pickleable exception base class.

    With an exception instance, iterate over its super classes (by MRO)
    and find the first super exception that's pickleable.  It does
    not go below :exc:`Exception` (i.e., it skips :exc:`Exception`,
    :class:`BaseException` and :class:`object`).  If that happens
    you should use :exc:`UnpickleableException` instead.

    Arguments:
        exc (BaseException): An exception instance.
        loads: decoder to use.
        dumps: encoder to use

    Returns:
        Exception: Nearest pickleable parent exception class
            (except :exc:`Exception` and parents), or if the exception is
            pickleable it will return :const:`None`.
    """
    exc_args = getattr(exc, 'args', [])
    for supercls in itermro(exc.__class__, unwanted_base_classes):
        try:
            superexc = supercls(*exc_args)
            loads(dumps(superexc))
        except Exception:  # pylint: disable=broad-except
            pass
        else:
            return superexc


def itermro(cls, stop):
    return takewhile(lambda sup: sup not in stop, getmro(cls))


def create_exception_cls(name, module, parent=None):
    """Dynamically create an exception class."""
    if not parent:
        parent = Exception
    return subclass_exception(name, parent, module)


def ensure_serializable(items, encoder):
    """Ensure items will serialize.

    For a given list of arbitrary objects, return the object
    or a string representation, safe for serialization.

    Arguments:
        items (Iterable[Any]): Objects to serialize.
        encoder (Callable): Callable function to serialize with.
    """
    safe_exc_args = []
    for arg in items:
        try:
            encoder(arg)
            safe_exc_args.append(arg)
        except Exception:  # pylint: disable=broad-except
            safe_exc_args.append(safe_repr(arg))
    return tuple(safe_exc_args)


@python_2_unicode_compatible
class UnpickleableExceptionWrapper(Exception):
    """Wraps unpickleable exceptions.

    Arguments:
        exc_module (str): See :attr:`exc_module`.
        exc_cls_name (str): See :attr:`exc_cls_name`.
        exc_args (Tuple[Any, ...]): See :attr:`exc_args`.

    Example:
        >>> def pickle_it(raising_function):
        ...     try:
        ...         raising_function()
        ...     except Exception as e:
        ...         exc = UnpickleableExceptionWrapper(
        ...             e.__class__.__module__,
        ...             e.__class__.__name__,
        ...             e.args,
        ...         )
        ...         pickle.dumps(exc)  # Works fine.
    """

    #: The module of the original exception.
    exc_module = None

    #: The name of the original exception class.
    exc_cls_name = None

    #: The arguments for the original exception.
    exc_args = None

    def __init__(self, exc_module, exc_cls_name, exc_args, text=None):
        safe_exc_args = ensure_serializable(exc_args, pickle.dumps)
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
    except Exception:  # pylint: disable=broad-except
        pass
    else:
        return exc
    nearest = find_pickleable_exception(exc)
    if nearest:
        return nearest
    return UnpickleableExceptionWrapper.from_exception(exc)


def get_pickleable_etype(cls, loads=pickle.loads, dumps=pickle.dumps):
    """Get pickleable exception type."""
    try:
        loads(dumps(cls))
    except Exception:  # pylint: disable=broad-except
        return Exception
    else:
        return cls


def get_pickled_exception(exc):
    """Reverse of :meth:`get_pickleable_exception`."""
    if isinstance(exc, UnpickleableExceptionWrapper):
        return exc.restore()
    return exc


def b64encode(s):
    return bytes_to_str(base64encode(str_to_bytes(s)))


def b64decode(s):
    return base64decode(str_to_bytes(s))


def strtobool(term, table={'false': False, 'no': False, '0': False,
                           'true': True, 'yes': True, '1': True,
                           'on': True, 'off': False}):
    """Convert common terms for true/false to bool.

    Examples (true/false/yes/no/on/off/1/0).
    """
    if isinstance(term, string_t):
        try:
            return table[term.lower()]
        except KeyError:
            raise TypeError('Cannot coerce {0!r} to type bool'.format(term))
    return term


def _datetime_to_json(dt):
    # See "Date Time String Format" in the ECMA-262 specification.
    if isinstance(dt, datetime.datetime):
        r = dt.isoformat()
        if dt.microsecond:
            r = r[:23] + r[26:]
        if r.endswith('+00:00'):
            r = r[:-6] + 'Z'
        return r
    elif isinstance(dt, datetime.time):
        r = dt.isoformat()
        if dt.microsecond:
            r = r[:12]
        return r
    else:
        return dt.isoformat()


def jsonify(obj,
            builtin_types=(numbers.Real, string_t), key=None,
            keyfilter=None,
            unknown_type_filter=None):
    """Transform object making it suitable for json serialization."""
    from kombu.abstract import Object as KombuDictType
    _jsonify = partial(jsonify, builtin_types=builtin_types, key=key,
                       keyfilter=keyfilter,
                       unknown_type_filter=unknown_type_filter)

    if isinstance(obj, KombuDictType):
        obj = obj.as_dict(recurse=True)

    if obj is None or isinstance(obj, builtin_types):
        return obj
    elif isinstance(obj, (tuple, list)):
        return [_jsonify(v) for v in obj]
    elif isinstance(obj, dict):
        return {
            k: _jsonify(v, key=k) for k, v in items(obj)
            if (keyfilter(k) if keyfilter else 1)
        }
    elif isinstance(obj, (datetime.date, datetime.time)):
        return _datetime_to_json(obj)
    elif isinstance(obj, datetime.timedelta):
        return str(obj)
    else:
        if unknown_type_filter is None:
            raise ValueError(
                'Unsupported type: {0!r} {1!r} (parent: {2})'.format(
                    type(obj), obj, key))
        return unknown_type_filter(obj)


# Since PyPy 3 targets Python 3.2, 'raise exc from None' will
# raise a TypeError so we need to look for Python 3.3 or newer
if PY33:  # pragma: no cover
    from vine.five import exec_
    _raise_with_context = None  # for flake8
    exec_("""def _raise_with_context(exc, ctx): raise exc from ctx""")

    def raise_with_context(exc):
        exc_info = sys.exc_info()
        if not exc_info:
            raise exc
        elif exc_info[1] is exc:
            raise
        _raise_with_context(exc, exc_info[1])
else:
    def raise_with_context(exc):
        exc_info = sys.exc_info()
        if not exc_info:
            raise exc
        if exc_info[1] is exc:
            raise
        elif exc_info[2]:
            reraise(type(exc), exc, exc_info[2])
        raise exc
