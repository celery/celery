from __future__ import with_statement
from contextlib import contextmanager
from StringIO import StringIO
from functools import wraps
import sys
import __builtin__


def _skip_test(reason, sign):

    def _wrap_test(fun):

        @wraps(fun)
        def _skipped_test(*args, **kwargs):
            sys.stderr.write("(%s: %s) " % (sign, reason))

        return _skipped_test
    return _wrap_test


def todo(reason):
    """TODO test decorator."""
    return _skip_test(reason, "TODO")


def skip(reason):
    """Skip test decorator."""
    return _skip_test(reason, "SKIP")


def skip_if(predicate, reason):
    """Skip test if predicate is ``True``."""

    def _inner(fun):
        return predicate and skip(reason)(fun) or fun

    return _inner


def skip_unless(predicate, reason):
    """Skip test if predicate is ``False``."""
    return skip_if(not predicate, reason)


@contextmanager
def mask_modules(*modnames):
    """Ban some modules from being importable inside the context

    For example:

        >>> with missing_modules("sys"):
        ...     try:
        ...         import sys
        ...     except ImportError:
        ...         print "sys not found"
        sys not found

        >>> import sys
        >>> sys.version
        (2, 5, 2, 'final', 0)

    """

    realimport = __builtin__.__import__

    def myimp(name, *args, **kwargs):
        if name in modnames:
            raise ImportError("No module named %s" % name)
        else:
            return realimport(name, *args, **kwargs)

    __builtin__.__import__ = myimp
    yield
    __builtin__.__import__ = realimport


@contextmanager
def override_stdouts():
    """Override ``sys.stdout`` and ``sys.stderr`` with ``StringIO``."""
    prev_out, prev_err = sys.stdout, sys.stderr
    mystdout, mystderr = StringIO(), StringIO()
    sys.stdout = sys.__stdout__ = mystdout
    sys.stderr = sys.__stderr__ = mystderr

    yield mystdout, mystderr

    sys.stdout = sys.__stdout__ = prev_out
    sys.stderr = sys.__stderr__ = prev_err
