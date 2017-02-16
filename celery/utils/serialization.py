# -*- coding: utf-8 -*-
"""Utilities for safely pickling exceptions."""
import datetime
import numbers
import sys

from base64 import b64encode as base64encode, b64decode as base64decode
from functools import partial
from inspect import getmro
from itertools import takewhile
from typing import Any, AnyStr, Callable, Optional, Sequence, Union

from kombu.utils.encoding import bytes_to_str, str_to_bytes

from .encoding import safe_repr

try:
    import cPickle as pickle
except ImportError:
    import pickle  # noqa

PY33 = sys.version_info >= (3, 3)

__all__ = [
    'UnpickleableExceptionWrapper', 'subclass_exception',
    'find_pickleable_exception', 'create_exception_cls',
    'get_pickleable_exception', 'get_pickleable_etype',
    'get_pickled_exception', 'strtobool',
]

#: List of base classes we probably don't want to reduce to.
try:
    unwanted_base_classes = (StandardError, Exception, BaseException, object)
except NameError:  # pragma: no cover
    unwanted_base_classes = (Exception, BaseException, object)  # py3k


def subclass_exception(name: str, parent: Any, module: str) -> Any:  # noqa
    """Create new exception class."""
    return type(name, (parent,), {'__module__': module})


def find_pickleable_exception(
        exc: Exception,
        loads: Callable[[AnyStr], Any]=pickle.loads,
        dumps: Callable[[Any], AnyStr]=pickle.dumps) -> Optional[Exception]:
    """Find first pickleable exception base class.

    With an exception instance, iterate over its super classes (by MRO)
    and find the first super exception that's pickleable.  It does
    not go below :exc:`Exception` (i.e., it skips :exc:`Exception`,
    :class:`BaseException` and :class:`object`).  If that happens
    you should use :exc:`UnpickleableException` instead.

    Arguments:
        exc (BaseException): An exception instance.

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


def itermro(cls: Any, stop: Any) -> Any:
    return takewhile(lambda sup: sup not in stop, getmro(cls))


def create_exception_cls(name: str, module: str,
                         parent: Optional[Any]=None) -> Exception:
    """Dynamically create an exception class."""
    if not parent:
        parent = Exception
    return subclass_exception(name, parent, module)


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
    exc_module: str = None

    #: The name of the original exception class.
    exc_cls_name: str = None

    #: The arguments for the original exception.
    exc_args: Sequence[Any] = None

    def __init__(self, exc_module: str, exc_cls_name: str,
                 exc_args: Sequence[Any], text: Optional[str]=None) -> None:
        safe_exc_args = []
        for arg in exc_args:
            try:
                pickle.dumps(arg)
                safe_exc_args.append(arg)
            except Exception:  # pylint: disable=broad-except
                safe_exc_args.append(safe_repr(arg))
        self.exc_module = exc_module
        self.exc_cls_name = exc_cls_name
        self.exc_args = safe_exc_args
        self.text = text
        Exception.__init__(self, exc_module, exc_cls_name, safe_exc_args, text)

    def restore(self) -> Exception:
        return create_exception_cls(self.exc_cls_name,
                                    self.exc_module)(*self.exc_args)

    def __str__(self) -> str:
        return self.text

    @classmethod
    def from_exception(cls, exc: Exception) -> 'UnpickleableExceptionWrapper':
        return cls(exc.__class__.__module__,
                   exc.__class__.__name__,
                   getattr(exc, 'args', []),
                   safe_repr(exc))


def get_pickleable_exception(exc: Exception) -> Exception:
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


def get_pickleable_etype(
        cls: Any,
        loads: Callable[[AnyStr], Any]=pickle.loads,
        dumps: Callable[[Any], AnyStr]=pickle.dumps) -> Exception:
    """Get pickleable exception type."""
    try:
        loads(dumps(cls))
    except Exception:  # pylint: disable=broad-except
        return Exception
    else:
        return cls


def get_pickled_exception(exc: Exception) -> Exception:
    """Reverse of :meth:`get_pickleable_exception`."""
    if isinstance(exc, UnpickleableExceptionWrapper):
        return exc.restore()
    return exc


def b64encode(s: AnyStr) -> str:
    return bytes_to_str(base64encode(str_to_bytes(s)))


def b64decode(s: AnyStr) -> bytes:
    return base64decode(str_to_bytes(s))


def strtobool(term: Union[str, bool],
              table={'false': False, 'no': False, '0': False,
                     'true': True, 'yes': True, '1': True,
                     'on': True, 'off': False}) -> bool:
    """Convert common terms for true/false to bool.

    Examples (true/false/yes/no/on/off/1/0).
    """
    if isinstance(term, str):
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


def jsonify(obj: Any,
            builtin_types=(numbers.Real, str), key: Optional[str]=None,
            keyfilter: Optional[Callable[[str], Any]]=None,
            unknown_type_filter: Optional[Callable[[Any], Any]]=None) -> Any:
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
            k: _jsonify(v, key=k) for k, v in obj.items()
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


def maybe_reraise() -> None:
    """Re-raise the current exception if any, or do nothing."""
    exc_info = sys.exc_info()
    try:
        if exc_info[2]:
            raise
    finally:
        # see http://docs.python.org/library/sys.html#sys.exc_info
        del(exc_info)
