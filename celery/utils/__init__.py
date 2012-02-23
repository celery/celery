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

import os
import sys
import operator
import imp as _imp
import importlib
import logging
import threading
import traceback
import warnings

from contextlib import contextmanager
from functools import partial, wraps
from inspect import getargspec
from itertools import islice
from pprint import pprint

from kombu.utils import cached_property, gen_unique_id, kwdict  # noqa
from kombu.utils import reprcall, reprkwargs                    # noqa
from kombu.utils.functional import promise, maybe_promise       # noqa
uuid = gen_unique_id

from ..exceptions import CPendingDeprecationWarning, CDeprecationWarning
from .compat import StringIO, reload

LOG_LEVELS = dict(logging._levelNames)
LOG_LEVELS["FATAL"] = logging.FATAL
LOG_LEVELS[logging.FATAL] = "FATAL"

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


class mpromise(promise):
    """Memoized promise.

    The function is only evaluated once, every subsequent access
    will return the same value.

    .. attribute:: evaluated

        Set to to :const:`True` after the promise has been evaluated.

    """
    evaluated = False
    _value = None

    def evaluate(self):
        if not self.evaluated:
            self._value = super(mpromise, self).evaluate()
            self.evaluated = True
        return self._value


def noop(*args, **kwargs):
    """No operation.

    Takes any arguments/keyword arguments and does nothing.

    """
    pass


def first(predicate, iterable):
    """Returns the first element in `iterable` that `predicate` returns a
    :const:`True` value for."""
    for item in iterable:
        if predicate(item):
            return item


def firstmethod(method):
    """Returns a functions that with a list of instances,
    finds the first instance that returns a value for the given method.

    The list can also contain promises (:class:`promise`.)

    """

    def _matcher(seq, *args, **kwargs):
        for cls in seq:
            try:
                answer = getattr(maybe_promise(cls), method)(*args, **kwargs)
                if answer is not None:
                    return answer
            except AttributeError:
                pass
    return _matcher


def chunks(it, n):
    """Split an iterator into chunks with `n` elements each.

    Examples

        # n == 2
        >>> x = chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), 2)
        >>> list(x)
        [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9], [10]]

        # n == 3
        >>> x = chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), 3)
        >>> list(x)
        [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]]

    """
    for first in it:
        yield [first] + list(islice(it, n - 1))


def padlist(container, size, default=None):
    """Pad list with default elements.

    Examples:

        >>> first, last, city = padlist(["George", "Costanza", "NYC"], 3)
        ("George", "Costanza", "NYC")
        >>> first, last, city = padlist(["George", "Costanza"], 3)
        ("George", "Costanza", None)
        >>> first, last, city, planet = padlist(["George", "Costanza",
                                                 "NYC"], 4, default="Earth")
        ("George", "Costanza", "NYC", "Earth")

    """
    return list(container)[:size] + [default] * (size - len(container))


def is_iterable(obj):
    try:
        iter(obj)
    except TypeError:
        return False
    return True


def mattrgetter(*attrs):
    """Like :func:`operator.itemgetter` but returns :const:`None` on missing
    attributes instead of raising :exc:`AttributeError`."""
    return lambda obj: dict((attr, getattr(obj, attr, None))
                                for attr in attrs)


if sys.version_info >= (3, 3):

    def qualname(obj):
        return obj.__qualname__

else:

    def qualname(obj):  # noqa
        if not hasattr(obj, "__name__") and hasattr(obj, "__class__"):
            return qualname(obj.__class__)

        return '.'.join([obj.__module__, obj.__name__])
get_full_cls_name = qualname  # XXX Compat


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


def get_cls_by_name(name, aliases={}, imp=None, package=None,
        sep='.', **kwargs):
    """Get class by name.

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

        >>> get_cls_by_name("celery.concurrency.processes.TaskPool")
        <class 'celery.concurrency.processes.TaskPool'>

        >>> get_cls_by_name("default", {
        ...     "default": "celery.concurrency.processes.TaskPool"})
        <class 'celery.concurrency.processes.TaskPool'>

        # Does not try to look up non-string names.
        >>> from celery.concurrency.processes import TaskPool
        >>> get_cls_by_name(TaskPool) is TaskPool
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
        module = imp(module_name, package=package, **kwargs)
    except ValueError, exc:
        raise ValueError, ValueError(
                "Couldn't import %r: %s" % (name, exc)), sys.exc_info()[2]
    return getattr(module, cls_name)

get_symbol_by_name = get_cls_by_name


def instantiate(name, *args, **kwargs):
    """Instantiate class by name.

    See :func:`get_cls_by_name`.

    """
    return get_cls_by_name(name)(*args, **kwargs)


def truncate_text(text, maxlen=128, suffix="..."):
    """Truncates text to a maximum number of characters."""
    if len(text) >= maxlen:
        return text[:maxlen].rsplit(" ", 1)[0] + suffix
    return text


def abbr(S, max, ellipsis="..."):
    if S is None:
        return "???"
    if len(S) > max:
        return ellipsis and (S[:max - len(ellipsis)] + ellipsis) or S[:max]
    return S


def abbrtask(S, max):
    if S is None:
        return "???"
    if len(S) > max:
        module, _, cls = S.rpartition(".")
        module = abbr(module, max - len(cls) - 3, False)
        return module + "[.]" + cls
    return S


def isatty(fh):
    # Fixes bug with mod_wsgi:
    #   mod_wsgi.Log object has no attribute isatty.
    return getattr(fh, "isatty", None) and fh.isatty()


def textindent(t, indent=0):
        """Indent text."""
        return "\n".join(" " * indent + p for p in t.split("\n"))


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


class NotAPackage(Exception):
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


def uniq(it):
    seen = set()
    for obj in it:
        if obj not in seen:
            yield obj
            seen.add(obj)


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
