# -*- coding: utf-8 -*-
"""
    celery.utils
    ~~~~~~~~~~~~

    Utility functions.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import operator
import sys
import threading
import traceback
import warnings

from functools import partial, wraps
from inspect import getargspec
from pprint import pprint

from ..exceptions import CPendingDeprecationWarning, CDeprecationWarning
from .compat import StringIO

from .imports import symbol_by_name, qualname
from .functional import noop

register_after_fork = symbol_by_name(
    "multiprocessing.util.register_after_fork", default=noop)

PENDING_DEPRECATION_FMT = """
    %(description)s is scheduled for deprecation in \
    version %(deprecation)s and removal in version v%(removal)s. \
    %(alternative)s
"""

DEPRECATION_FMT = """
    %(description)s is deprecated and scheduled for removal in
    version %(removal)s. %(alternative)s
"""


def warn_deprecated(description=None, deprecation=None, removal=None,
        alternative=None):
    ctx = {"description": description,
           "deprecation": deprecation, "removal": removal,
           "alternative": alternative}
    if deprecation is not None:
        w = CPendingDeprecationWarning(PENDING_DEPRECATION_FMT % ctx)
    else:
        w = CDeprecationWarning(DEPRECATION_FMT % ctx)
    warnings.warn(w)


def deprecated(description=None, deprecation=None, removal=None,
        alternative=None):

    def _inner(fun):

        @wraps(fun)
        def __inner(*args, **kwargs):
            warn_deprecated(description=description or qualname(fun),
                            deprecation=deprecation,
                            removal=removal,
                            alternative=alternative)
            return fun(*args, **kwargs)
        return __inner
    return _inner


def lpmerge(L, R):
    """Left precedent dictionary merge.  Keeps values from `l`, if the value
    in `r` is :const:`None`."""
    return dict(L, **dict((k, v) for k, v in R.iteritems() if v is not None))


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
        >>> fun_takes_kwargs(foo, ["logfile", "loglevel", "task_id"])
        ["logfile", "loglevel"]

        >>> def foo(self, x, y, **kwargs):
        >>> fun_takes_kwargs(foo, ["logfile", "loglevel", "task_id"])
        ["logfile", "loglevel", "task_id"]

    """
    argspec = getattr(fun, "argspec", getargspec(fun))
    args, _varargs, keywords, _defaults = argspec
    if keywords != None:
        return kwlist
    return filter(partial(operator.contains, args), kwlist)


def isatty(fh):
    # Fixes bug with mod_wsgi:
    #   mod_wsgi.Log object has no attribute isatty.
    return getattr(fh, "isatty", None) and fh.isatty()


def cry():  # pragma: no cover
    """Return stacktrace of all active threads.

    From https://gist.github.com/737056

    """
    tmap = {}
    main_thread = None
    # get a map of threads by their ID so we can print their names
    # during the traceback dump
    for t in threading.enumerate():
        if getattr(t, "ident", None):
            tmap[t.ident] = t
        else:
            main_thread = t

    out = StringIO()
    sep = "=" * 49 + "\n"
    for tid, frame in sys._current_frames().iteritems():
        thread = tmap.get(tid, main_thread)
        if not thread:
            # skip old junk (left-overs from a fork)
            continue
        out.write("%s\n" % (thread.getName(), ))
        out.write(sep)
        traceback.print_stack(frame, file=out)
        out.write(sep)
        out.write("LOCAL VARIABLES\n")
        out.write(sep)
        pprint(frame.f_locals, stream=out)
        out.write("\n\n")
    return out.getvalue()


def maybe_reraise():
    """Reraise if an exception is currently being handled, or return
    otherwise."""
    type_, exc, tb = sys.exc_info()
    try:
        if tb:
            raise type_, exc, tb
    finally:
        # see http://docs.python.org/library/sys.html#sys.exc_info
        del(tb)


# - XXX Compat
from .log import LOG_LEVELS     # noqa
from .imports import (          # noqa
        qualname as get_full_cls_name, symbol_by_name as get_cls_by_name,
        instantiate, import_from_cwd
)
from .functional import chunks, noop            # noqa
from kombu.utils import cached_property, uuid   # noqa
gen_unique_id = uuid
