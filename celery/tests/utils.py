from __future__ import with_statement

import os
import sys
import __builtin__
from StringIO import StringIO
from functools import wraps
from contextlib import contextmanager

from celery.utils import noop


def with_environ(env_name, env_value):

    def _envpatched(fun):

        @wraps(fun)
        def _patch_environ(*args, **kwargs):
            prev_val = os.environ.get(env_name)
            os.environ[env_name] = env_value
            try:
                return fun(*args, **kwargs)
            finally:
                if prev_val is not None:
                    os.environ[env_name] = prev_val

        return _patch_environ
    return _envpatched


def sleepdeprived(fun):

    @wraps(fun)
    def _sleepdeprived(*args, **kwargs):
        import time
        old_sleep = time.sleep
        time.sleep = noop
        try:
            return fun(*args, **kwargs)
        finally:
            time.sleep = old_sleep

    return _sleepdeprived


def skip_if_environ(env_var_name):

    def _wrap_test(fun):

        @wraps(fun)
        def _skips_if_environ(*args, **kwargs):
            if os.environ.get(env_var_name):
                sys.stderr.write("SKIP %s: %s set\n" % (
                    fun.__name__, env_var_name))
                return
            return fun(*args, **kwargs)

        return _skips_if_environ

    return _wrap_test


def skip_if_quick(fun):
    return skip_if_environ("QUICKTEST")(fun)


def _skip_test(reason, sign):

    def _wrap_test(fun):

        @wraps(fun)
        def _skipped_test(*args, **kwargs):
            sys.stderr.write("%s: %s " % (sign, reason))

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



# Taken from
# http://bitbucket.org/runeh/snippets/src/tip/missing_modules.py
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
