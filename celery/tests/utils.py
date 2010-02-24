from __future__ import with_statement, generators

import os
import sys
import __builtin__
from StringIO import StringIO
from functools import wraps

class GeneratorContextManager(object):
    def __init__(self, gen):
        self.gen = gen

    def __enter__(self):
        try:
            return self.gen.next()
        except StopIteration:
            raise RuntimeError("generator didn't yield")

    def __exit__(self, type, value, traceback):
        if type is None:
            try:
                self.gen.next()
            except StopIteration:
                return
            else:
                raise RuntimeError("generator didn't stop")
        else:
            try:
                self.gen.throw(type, value, traceback)
                raise RuntimeError("generator didn't stop after throw()")
            except StopIteration:
                return True
            except:
                if sys.exc_info()[1] is not value:
                    raise

def fallback_contextmanager(fun):
    def helper(*args, **kwds):
        return GeneratorContextManager(fun(*args, **kwds))
    return helper


def execute_context(context, fun):
    val = context.__enter__()
    exc_info = (None, None, None)
    retval = None
    try:
        retval = fun(val)
    except:
        exc_info = sys.exc_info()
    context.__exit__(*exc_info)
    return retval


try:
    from contextlib import contextmanager
except ImportError:
    contextmanager = fallback_contextmanager

from celery.utils import noop


@contextmanager
def eager_tasks():

    from celery import conf
    prev = conf.ALWAYS_EAGER
    conf.ALWAYS_EAGER = True

    yield True

    conf.ALWAYS_EAGER = prev


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
    yield True
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
