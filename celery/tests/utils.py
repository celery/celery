from __future__ import generators

try:
    import unittest
    unittest.skip
except AttributeError:
    import unittest2 as unittest

import importlib
import logging
import os
import sys
import time
try:
    import __builtin__ as builtins
except ImportError:    # py3k
    import builtins

from celery.utils.compat import StringIO, LoggerAdapter
try:
    from contextlib import contextmanager
except ImportError:
    from celery.tests.utils import fallback_contextmanager as contextmanager


from nose import SkipTest

from celery.app import app_or_default
from celery.utils.functional import wraps


class AppCase(unittest.TestCase):

    def setUp(self):
        from celery.app import current_app
        self._current_app = current_app()
        self.setup()

    def tearDown(self):
        self.teardown()
        self._current_app.set_current()

    def setup(self):
        pass

    def teardown(self):
        pass


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
            except AttributeError:
                raise value
            except:
                if sys.exc_info()[1] is not value:
                    raise


def get_handlers(logger):
    if isinstance(logger, LoggerAdapter):
        return logger.logger.handlers
    return logger.handlers


def set_handlers(logger, new_handlers):
    if isinstance(logger, LoggerAdapter):
        logger.logger.handlers = new_handlers
    logger.handlers = new_handlers


@contextmanager
def wrap_logger(logger, loglevel=logging.ERROR):
    old_handlers = get_handlers(logger)
    sio = StringIO()
    siohandler = logging.StreamHandler(sio)
    set_handlers(logger, [siohandler])

    yield sio

    set_handlers(logger, old_handlers)


def fallback_contextmanager(fun):
    def helper(*args, **kwds):
        return GeneratorContextManager(fun(*args, **kwds))
    return helper


def execute_context(context, fun):
    val = context.__enter__()
    exc_info = (None, None, None)
    try:
        try:
            return fun(val)
        except:
            exc_info = sys.exc_info()
            raise
    finally:
        context.__exit__(*exc_info)


try:
    from contextlib import contextmanager
except ImportError:
    contextmanager = fallback_contextmanager

from celery.utils import noop


@contextmanager
def eager_tasks():
    app = app_or_default()

    prev = app.conf.CELERY_ALWAYS_EAGER
    app.conf.CELERY_ALWAYS_EAGER = True

    yield True

    app.conf.CELERY_ALWAYS_EAGER = prev


def with_eager_tasks(fun):

    @wraps(fun)
    def _inner(*args, **kwargs):
        app = app_or_default()
        prev = app.conf.CELERY_ALWAYS_EAGER
        app.conf.CELERY_ALWAYS_EAGER = True
        try:
            return fun(*args, **kwargs)
        finally:
            app.conf.CELERY_ALWAYS_EAGER = prev


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


def sleepdeprived(module=time):

    def _sleepdeprived(fun):

        @wraps(fun)
        def __sleepdeprived(*args, **kwargs):
            old_sleep = module.sleep
            module.sleep = noop
            try:
                return fun(*args, **kwargs)
            finally:
                module.sleep = old_sleep

        return __sleepdeprived

    return _sleepdeprived


def skip_if_environ(env_var_name):

    def _wrap_test(fun):

        @wraps(fun)
        def _skips_if_environ(*args, **kwargs):
            if os.environ.get(env_var_name):
                raise SkipTest("SKIP %s: %s set\n" % (
                    fun.__name__, env_var_name))
            return fun(*args, **kwargs)

        return _skips_if_environ

    return _wrap_test


def skip_if_quick(fun):
    return skip_if_environ("QUICKTEST")(fun)


def _skip_test(reason, sign):

    def _wrap_test(fun):

        @wraps(fun)
        def _skipped_test(*args, **kwargs):
            raise SkipTest("%s: %s" % (sign, reason))

        return _skipped_test
    return _wrap_test


def todo(reason):
    """TODO test decorator."""
    return _skip_test(reason, "TODO")


def skip(reason):
    """Skip test decorator."""
    return _skip_test(reason, "SKIP")


def skip_if(predicate, reason):
    """Skip test if predicate is :const:`True`."""

    def _inner(fun):
        return predicate and skip(reason)(fun) or fun

    return _inner


def skip_unless(predicate, reason):
    """Skip test if predicate is :const:`False`."""
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

    realimport = builtins.__import__

    def myimp(name, *args, **kwargs):
        if name in modnames:
            raise ImportError("No module named %s" % name)
        else:
            return realimport(name, *args, **kwargs)

    builtins.__import__ = myimp
    yield True
    builtins.__import__ = realimport


@contextmanager
def override_stdouts():
    """Override `sys.stdout` and `sys.stderr` with `StringIO`."""
    prev_out, prev_err = sys.stdout, sys.stderr
    mystdout, mystderr = StringIO(), StringIO()
    sys.stdout = sys.__stdout__ = mystdout
    sys.stderr = sys.__stderr__ = mystderr

    yield mystdout, mystderr

    sys.stdout = sys.__stdout__ = prev_out
    sys.stderr = sys.__stderr__ = prev_err


def patch(module, name, mocked):
    module = importlib.import_module(module)

    def _patch(fun):

        @wraps(fun)
        def __patched(*args, **kwargs):
            prev = getattr(module, name)
            setattr(module, name, mocked)
            try:
                return fun(*args, **kwargs)
            finally:
                setattr(module, name, prev)
        return __patched
    return _patch
