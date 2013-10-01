# -*- coding: utf-8 -*-
"""
    celery.utils
    ~~~~~~~~~~~~

    Utility functions.

"""
from __future__ import absolute_import, print_function

import os
import sys
import traceback
import warnings
import datetime

from functools import partial, wraps
from inspect import getargspec
from pprint import pprint

from kombu.entity import Exchange, Queue

from celery.exceptions import CPendingDeprecationWarning, CDeprecationWarning
from celery.five import StringIO, items, reraise, string_t

__all__ = ['worker_direct', 'warn_deprecated', 'deprecated', 'lpmerge',
           'is_iterable', 'isatty', 'cry', 'maybe_reraise', 'strtobool',
           'jsonify', 'gen_task_name', 'nodename', 'nodesplit',
           'cached_property']


PENDING_DEPRECATION_FMT = """
    {description} is scheduled for deprecation in \
    version {deprecation} and removal in version v{removal}. \
    {alternative}
"""

DEPRECATION_FMT = """
    {description} is deprecated and scheduled for removal in
    version {removal}. {alternative}
"""

#: Billiard sets this when execv is enabled.
#: We use it to find out the name of the original ``__main__``
#: module, so that we can properly rewrite the name of the
#: task to be that of ``App.main``.
MP_MAIN_FILE = os.environ.get('MP_MAIN_FILE') or None

#: Exchange for worker direct queues.
WORKER_DIRECT_EXCHANGE = Exchange('C.dq')

#: Format for worker direct queue names.
WORKER_DIRECT_QUEUE_FORMAT = '{hostname}.dq'

#: Separator for worker node name and hostname.
NODENAME_SEP = '@'


def worker_direct(hostname):
    """Return :class:`kombu.Queue` that is a direct route to
    a worker by hostname.

    :param hostname: The fully qualified node name of a worker
                     (e.g. ``w1@example.com``).  If passed a
                     :class:`kombu.Queue` instance it will simply return
                     that instead.
    """
    if isinstance(hostname, Queue):
        return hostname
    return Queue(WORKER_DIRECT_QUEUE_FORMAT.format(hostname=hostname),
                 WORKER_DIRECT_EXCHANGE,
                 hostname, auto_delete=True)


def warn_deprecated(description=None, deprecation=None,
                    removal=None, alternative=None):
    ctx = {'description': description,
           'deprecation': deprecation, 'removal': removal,
           'alternative': alternative}
    if deprecation is not None:
        w = CPendingDeprecationWarning(PENDING_DEPRECATION_FMT.format(**ctx))
    else:
        w = CDeprecationWarning(DEPRECATION_FMT.format(**ctx))
    warnings.warn(w)


def deprecated(description=None, deprecation=None,
               removal=None, alternative=None):
    """Decorator for deprecated functions.

    A deprecation warning will be emitted when the function is called.

    :keyword description: Description of what is being deprecated.
    :keyword deprecation: Version that marks first deprecation, if this
      argument is not set a ``PendingDeprecationWarning`` will be emitted
      instead.
    :keyword removed:  Future version when this feature will be removed.
    :keyword alternative:  Instructions for an alternative solution (if any).

    """
    def _inner(fun):

        @wraps(fun)
        def __inner(*args, **kwargs):
            from .imports import qualname
            warn_deprecated(description=description or qualname(fun),
                            deprecation=deprecation,
                            removal=removal,
                            alternative=alternative)
            return fun(*args, **kwargs)
        return __inner
    return _inner


def lpmerge(L, R):
    """In place left precedent dictionary merge.

    Keeps values from `L`, if the value in `R` is :const:`None`."""
    set = L.__setitem__
    [set(k, v) for k, v in items(R) if v is not None]
    return L


def is_iterable(obj):
    try:
        iter(obj)
    except TypeError:
        return False
    return True


def fun_takes_kwargs(fun, kwlist=[]):
    """With a function, and a list of keyword arguments, returns arguments
    in the list which the function takes.

    If the object has an `argspec` attribute that is used instead
    of using the :meth:`inspect.getargspec` introspection.

    :param fun: The function to inspect arguments of.
    :param kwlist: The list of keyword arguments.

    Examples

        >>> def foo(self, x, y, logfile=None, loglevel=None):
        ...     return x * y
        >>> fun_takes_kwargs(foo, ['logfile', 'loglevel', 'task_id'])
        ['logfile', 'loglevel']

        >>> def foo(self, x, y, **kwargs):
        >>> fun_takes_kwargs(foo, ['logfile', 'loglevel', 'task_id'])
        ['logfile', 'loglevel', 'task_id']

    """
    S = getattr(fun, 'argspec', getargspec(fun))
    if S.keywords is not None:
        return kwlist
    return [kw for kw in kwlist if kw in S.args]


def isatty(fh):
    try:
        return fh.isatty()
    except AttributeError:
        pass


def cry(out=None, sepchr='=', seplen=49):  # pragma: no cover
    """Return stacktrace of all active threads.

    From https://gist.github.com/737056

    """
    import threading

    out = StringIO() if out is None else out
    P = partial(print, file=out)

    # get a map of threads by their ID so we can print their names
    # during the traceback dump
    tmap = dict((t.ident, t) for t in threading.enumerate())

    sep = sepchr * seplen
    for tid, frame in items(sys._current_frames()):
        thread = tmap.get(tid)
        if not thread:
            # skip old junk (left-overs from a fork)
            continue
        P('{0.name}'.format(thread))
        P(sep)
        traceback.print_stack(frame, file=out)
        P(sep)
        P('LOCAL VARIABLES')
        P(sep)
        pprint(frame.f_locals, stream=out)
        P('\n')
    return out.getvalue()


def maybe_reraise():
    """Re-raise if an exception is currently being handled, or return
    otherwise."""
    exc_info = sys.exc_info()
    try:
        if exc_info[2]:
            reraise(exc_info[0], exc_info[1], exc_info[2])
    finally:
        # see http://docs.python.org/library/sys.html#sys.exc_info
        del(exc_info)


def strtobool(term, table={'false': False, 'no': False, '0': False,
                           'true': True, 'yes': True, '1': True,
                           'on': True, 'off': False}):
    """Convert common terms for true/false to bool
    (true/false/yes/no/on/off/1/0)."""
    if isinstance(term, string_t):
        try:
            return table[term.lower()]
        except KeyError:
            raise TypeError('Cannot coerce {0!r} to type bool'.format(term))
    return term


def jsonify(obj,
            builtin_types=(int, float, string_t), key=None,
            keyfilter=None,
            unknown_type_filter=None):
    """Transforms object making it suitable for json serialization"""
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
        return dict((k, _jsonify(v, key=k))
                    for k, v in items(obj)
                    if (keyfilter(k) if keyfilter else 1))
    elif isinstance(obj, datetime.datetime):
        # See "Date Time String Format" in the ECMA-262 specification.
        r = obj.isoformat()
        if obj.microsecond:
            r = r[:23] + r[26:]
        if r.endswith('+00:00'):
            r = r[:-6] + 'Z'
        return r
    elif isinstance(obj, datetime.date):
        return obj.isoformat()
    elif isinstance(obj, datetime.time):
        r = obj.isoformat()
        if obj.microsecond:
            r = r[:12]
        return r
    elif isinstance(obj, datetime.timedelta):
        return str(obj)
    else:
        if unknown_type_filter is None:
            raise ValueError(
                'Unsupported type: {0!r} {1!r} (parent: {2})'.format(
                    type(obj), obj, key))
        return unknown_type_filter(obj)


def gen_task_name(app, name, module_name):
    """Generate task name from name/module pair."""
    try:
        module = sys.modules[module_name]
    except KeyError:
        # Fix for manage.py shell_plus (Issue #366)
        module = None

    if module is not None:
        module_name = module.__name__
        # - If the task module is used as the __main__ script
        # - we need to rewrite the module part of the task name
        # - to match App.main.
        if MP_MAIN_FILE and module.__file__ == MP_MAIN_FILE:
            # - see comment about :envvar:`MP_MAIN_FILE` above.
            module_name = '__main__'
    if module_name == '__main__' and app.main:
        return '.'.join([app.main, name])
    return '.'.join(p for p in (module_name, name) if p)


def nodename(name, hostname):
    """Create node name from name/hostname pair."""
    return NODENAME_SEP.join((name, hostname))


def nodesplit(nodename):
    """Split node name into tuple of name/hostname."""
    parts = nodename.split(NODENAME_SEP, 1)
    if len(parts) == 1:
        return None, parts[0]
    return parts


# ------------------------------------------------------------------------ #
# > XXX Compat
from .log import LOG_LEVELS     # noqa
from .imports import (          # noqa
    qualname as get_full_cls_name, symbol_by_name as get_cls_by_name,
    instantiate, import_from_cwd
)
from .functional import chunks, noop                    # noqa
from kombu.utils import cached_property, kwdict, uuid   # noqa
gen_unique_id = uuid
