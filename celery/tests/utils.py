from __future__ import absolute_import
from __future__ import with_statement

try:
    import unittest  # noqa
    unittest.skip
    from unittest.util import safe_repr, unorderable_list_difference
except AttributeError:
    import unittest2 as unittest  # noqa
    from unittest2.util import safe_repr, unorderable_list_difference  # noqa

import importlib
import logging
import os
import platform
import re
import sys
import time
import warnings
try:
    import __builtin__ as builtins
except ImportError:  # py3k
    import builtins  # noqa

from contextlib import contextmanager
from functools import partial, wraps
from types import ModuleType

import mock
from nose import SkipTest
from kombu.log import NullHandler
from kombu.utils import nested

from celery.app import app_or_default
from celery.utils.compat import WhateverIO
from celery.utils.functional import noop

from .compat import catch_warnings


class Mock(mock.Mock):

    def __init__(self, *args, **kwargs):
        attrs = kwargs.pop('attrs', None) or {}
        super(Mock, self).__init__(*args, **kwargs)
        for attr_name, attr_value in attrs.items():
            setattr(self, attr_name, attr_value)


def skip_unless_module(module):

    def _inner(fun):

        @wraps(fun)
        def __inner(*args, **kwargs):
            try:
                importlib.import_module(module)
            except ImportError:
                raise SkipTest('Does not have %s' % (module, ))

            return fun(*args, **kwargs)

        return __inner
    return _inner


# -- adds assertWarns from recent unittest2, not in Python 2.7.

class _AssertRaisesBaseContext(object):

    def __init__(self, expected, test_case, callable_obj=None,
                 expected_regex=None):
        self.expected = expected
        self.failureException = test_case.failureException
        self.obj_name = None
        if isinstance(expected_regex, basestring):
            expected_regex = re.compile(expected_regex)
        self.expected_regex = expected_regex


class _AssertWarnsContext(_AssertRaisesBaseContext):
    """A context manager used to implement TestCase.assertWarns* methods."""

    def __enter__(self):
        # The __warningregistry__'s need to be in a pristine state for tests
        # to work properly.
        warnings.resetwarnings()
        for v in sys.modules.values():
            if getattr(v, '__warningregistry__', None):
                v.__warningregistry__ = {}
        self.warnings_manager = catch_warnings(record=True)
        self.warnings = self.warnings_manager.__enter__()
        warnings.simplefilter('always', self.expected)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.warnings_manager.__exit__(exc_type, exc_value, tb)
        if exc_type is not None:
            # let unexpected exceptions pass through
            return
        try:
            exc_name = self.expected.__name__
        except AttributeError:
            exc_name = str(self.expected)
        first_matching = None
        for m in self.warnings:
            w = m.message
            if not isinstance(w, self.expected):
                continue
            if first_matching is None:
                first_matching = w
            if (self.expected_regex is not None and
                    not self.expected_regex.search(str(w))):
                continue
            # store warning for later retrieval
            self.warning = w
            self.filename = m.filename
            self.lineno = m.lineno
            return
        # Now we simply try to choose a helpful failure message
        if first_matching is not None:
            raise self.failureException(
                '%r does not match %r' % (
                    self.expected_regex.pattern, str(first_matching)))
        if self.obj_name:
            raise self.failureException(
                '%s not triggered by %s' % (exc_name, self.obj_name))
        else:
            raise self.failureException('%s not triggered' % exc_name)


class Case(unittest.TestCase):

    def assertWarns(self, expected_warning):
        return _AssertWarnsContext(expected_warning, self, None)

    def assertWarnsRegex(self, expected_warning, expected_regex):
        return _AssertWarnsContext(expected_warning, self,
                                   None, expected_regex)

    def assertDictContainsSubset(self, expected, actual, msg=None):
        missing, mismatched = [], []

        for key, value in expected.iteritems():
            if key not in actual:
                missing.append(key)
            elif value != actual[key]:
                mismatched.append('%s, expected: %s, actual: %s' % (
                    safe_repr(key), safe_repr(value),
                    safe_repr(actual[key])))

        if not (missing or mismatched):
            return

        standard_msg = ''
        if missing:
            standard_msg = 'Missing: %s' % ','.join(map(safe_repr, missing))

        if mismatched:
            if standard_msg:
                standard_msg += '; '
            standard_msg += 'Mismatched values: %s' % (
                ','.join(mismatched))

        self.fail(self._formatMessage(msg, standard_msg))

    def assertItemsEqual(self, expected_seq, actual_seq, msg=None):
        missing = unexpected = None
        try:
            expected = sorted(expected_seq)
            actual = sorted(actual_seq)
        except TypeError:
            # Unsortable items (example: set(), complex(), ...)
            expected = list(expected_seq)
            actual = list(actual_seq)
            missing, unexpected = unorderable_list_difference(
                expected, actual)
        else:
            return self.assertSequenceEqual(expected, actual, msg=msg)

        errors = []
        if missing:
            errors.append(
                'Expected, but missing:\n    %s' % (safe_repr(missing), ),
            )
        if unexpected:
            errors.append(
                'Unexpected, but present:\n    %s' % (safe_repr(unexpected), ),
            )
        if errors:
            standardMsg = '\n'.join(errors)
            self.fail(self._formatMessage(msg, standardMsg))


class AppCase(Case):

    def setUp(self):
        from celery.app import current_app
        from celery.backends.cache import CacheBackend, DummyClient
        app = self.app = self._current_app = current_app()
        if isinstance(app.backend, CacheBackend):
            if isinstance(app.backend.client, DummyClient):
                app.backend.client.cache.clear()
        app.backend._cache.clear()
        self.setup()

    def tearDown(self):
        self.teardown()
        self._current_app.set_current()

    def setup(self):
        pass

    def teardown(self):
        pass


def get_handlers(logger):
    return [h for h in logger.handlers if not isinstance(h, NullHandler)]


@contextmanager
def wrap_logger(logger, loglevel=logging.ERROR):
    old_handlers = get_handlers(logger)
    sio = WhateverIO()
    siohandler = logging.StreamHandler(sio)
    logger.handlers = [siohandler]

    try:
        yield sio
    finally:
        logger.handlers = old_handlers


@contextmanager
def eager_tasks():
    app = app_or_default()

    prev = app.conf.CELERY_ALWAYS_EAGER
    app.conf.CELERY_ALWAYS_EAGER = True
    try:
        yield True
    finally:
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
                raise SkipTest('SKIP %s: %s set\n' % (
                    fun.__name__, env_var_name))
            return fun(*args, **kwargs)

        return _skips_if_environ

    return _wrap_test


def skip_if_quick(fun):
    return skip_if_environ('QUICKTEST')(fun)


def _skip_test(reason, sign):

    def _wrap_test(fun):

        @wraps(fun)
        def _skipped_test(*args, **kwargs):
            raise SkipTest('%s: %s' % (sign, reason))

        return _skipped_test
    return _wrap_test


def todo(reason):
    """TODO test decorator."""
    return _skip_test(reason, 'TODO')


def skip(reason):
    """Skip test decorator."""
    return _skip_test(reason, 'SKIP')


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

        >>> with missing_modules('sys'):
        ...     try:
        ...         import sys
        ...     except ImportError:
        ...         print 'sys not found'
        sys not found

        >>> import sys
        >>> sys.version
        (2, 5, 2, 'final', 0)

    """

    realimport = builtins.__import__

    def myimp(name, *args, **kwargs):
        if name in modnames:
            raise ImportError('No module named %s' % name)
        else:
            return realimport(name, *args, **kwargs)

    builtins.__import__ = myimp
    try:
        yield True
    finally:
        builtins.__import__ = realimport


@contextmanager
def override_stdouts():
    """Override `sys.stdout` and `sys.stderr` with `WhateverIO`."""
    prev_out, prev_err = sys.stdout, sys.stderr
    mystdout, mystderr = WhateverIO(), WhateverIO()
    sys.stdout = sys.__stdout__ = mystdout
    sys.stderr = sys.__stderr__ = mystderr

    try:
        yield mystdout, mystderr
    finally:
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


@contextmanager
def replace_module_value(module, name, value=None):
    has_prev = hasattr(module, name)
    prev = getattr(module, name, None)
    if value:
        setattr(module, name, value)
    else:
        try:
            delattr(module, name)
        except AttributeError:
            pass
    try:
        yield
    finally:
        if prev is not None:
            setattr(sys, name, prev)
        if not has_prev:
            try:
                delattr(module, name)
            except AttributeError:
                pass
pypy_version = partial(
    replace_module_value, sys, 'pypy_version_info',
)
platform_pyimp = partial(
    replace_module_value, platform, 'python_implementation',
)


@contextmanager
def sys_platform(value):
    prev, sys.platform = sys.platform, value
    try:
        yield
    finally:
        sys.platform = prev


@contextmanager
def reset_modules(*modules):
    prev = dict((k, sys.modules.pop(k)) for k in modules if k in sys.modules)
    try:
        yield
    finally:
        sys.modules.update(prev)


@contextmanager
def patch_modules(*modules):
    prev = {}
    for mod in modules:
        prev[mod], sys.modules[mod] = sys.modules[mod], ModuleType(mod)
    try:
        yield
    finally:
        for name, mod in prev.iteritems():
            if mod is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod


@contextmanager
def mock_module(*names):
    prev = {}

    class MockModule(ModuleType):

        def __getattr__(self, attr):
            setattr(self, attr, Mock())
            return ModuleType.__getattribute__(self, attr)

    mods = []
    for name in names:
        try:
            prev[name] = sys.modules[name]
        except KeyError:
            pass
        mod = sys.modules[name] = MockModule(name)
        mods.append(mod)
    try:
        yield mods
    finally:
        for name in names:
            try:
                sys.modules[name] = prev[name]
            except KeyError:
                try:
                    del(sys.modules[name])
                except KeyError:
                    pass


@contextmanager
def mock_context(mock, typ=Mock):
    context = mock.return_value = Mock()
    context.__enter__ = typ()
    context.__exit__ = typ()

    def on_exit(*x):
        if x[0]:
            raise x[0], x[1], x[2]
    context.__exit__.side_effect = on_exit
    context.__enter__.return_value = context
    try:
        yield context
    finally:
        context.reset()


@contextmanager
def mock_open(typ=WhateverIO, side_effect=None):
    with mock.patch('__builtin__.open') as open_:
        with mock_context(open_) as context:
            if side_effect is not None:
                context.__enter__.side_effect = side_effect
            val = context.__enter__.return_value = typ()
            val.__exit__ = Mock()
            yield val


def patch_many(*targets):
    return nested(*[mock.patch(target) for target in targets])


@contextmanager
def patch_settings(app=None, **config):
    if app is None:
        from celery import current_app
        app = current_app
    prev = {}
    for key, value in config.iteritems():
        try:
            prev[key] = getattr(app.conf, key)
        except AttributeError:
            pass
        setattr(app.conf, key, value)

    try:
        yield app.conf
    finally:
        for key, value in prev.iteritems():
            setattr(app.conf, key, value)


@contextmanager
def assert_signal_called(signal, **expected):
    handler = Mock()
    call_handler = partial(handler)
    signal.connect(call_handler)
    try:
        yield handler
    finally:
        signal.disconnect(call_handler)
    handler.assert_called_with(signal=signal, **expected)


def skip_if_pypy(fun):

    @wraps(fun)
    def _inner(*args, **kwargs):
        if getattr(sys, 'pypy_version_info', None):
            raise SkipTest('does not work on PyPy')
        return fun(*args, **kwargs)
    return _inner


def skip_if_jython(fun):

    @wraps(fun)
    def _inner(*args, **kwargs):
        if sys.platform.startswith('java'):
            raise SkipTest('does not work on Jython')
        return fun(*args, **kwargs)
    return _inner
