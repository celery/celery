from __future__ import absolute_import, unicode_literals

import importlib
import inspect
import io
import logging
import os
import platform
import re
import sys
import time
import types
import warnings

from contextlib import contextmanager
from functools import partial, wraps
from six import (
    iteritems as items,
    itervalues as values,
    string_types,
    reraise,
)
from six.moves import builtins

from nose import SkipTest

try:
    import unittest  # noqa
    unittest.skip
    from unittest.util import safe_repr, unorderable_list_difference
except AttributeError:
    import unittest2 as unittest  # noqa
    from unittest2.util import safe_repr, unorderable_list_difference  # noqa

try:
    from unittest import mock
except ImportError:
    import mock  # noqa

__all__ = [
    'ANY', 'Case', 'ContextMock', 'MagicMock', 'Mock', 'MockCallbacks',
    'call', 'patch', 'sentinel',

    'mock_open', 'mock_context', 'mock_module',
    'patch_modules', 'reset_modules', 'sys_platform', 'pypy_version',
    'platform_pyimp', 'replace_module_value', 'override_stdouts',
    'mask_modules', 'sleepdeprived', 'mock_environ', 'wrap_logger',
    'restore_logging',

    'todo', 'skip', 'skip_if_darwin', 'skip_if_environ',
    'skip_if_jython', 'skip_if_platform', 'skip_if_pypy', 'skip_if_python3',
    'skip_if_win32', 'skip_unless_module', 'skip_unless_symbol',
]

patch = mock.patch
call = mock.call
sentinel = mock.sentinel
MagicMock = mock.MagicMock
ANY = mock.ANY

PY3 = sys.version_info[0] == 3
if PY3:
    open_fqdn = 'builtins.open'
    module_name_t = str
else:
    open_fqdn = '__builtin__.open'  # noqa
    module_name_t = bytes  # noqa

StringIO = io.StringIO
_SIO_write = StringIO.write
_SIO_init = StringIO.__init__


def symbol_by_name(name, aliases={}, imp=None, package=None,
                   sep='.', default=None, **kwargs):
    """Get symbol by qualified name.

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

        >>> symbol_by_name('celery.concurrency.processes.TaskPool')
        <class 'celery.concurrency.processes.TaskPool'>

        >>> symbol_by_name('default', {
        ...     'default': 'celery.concurrency.processes.TaskPool'})
        <class 'celery.concurrency.processes.TaskPool'>

        # Does not try to look up non-string names.
        >>> from celery.concurrency.processes import TaskPool
        >>> symbol_by_name(TaskPool) is TaskPool
        True

    """
    if imp is None:
        imp = importlib.import_module

    if not isinstance(name, string_types):
        return name                                 # already a class

    name = aliases.get(name) or name
    sep = ':' if ':' in name else sep
    module_name, _, cls_name = name.rpartition(sep)
    if not module_name:
        cls_name, module_name = None, package if package else cls_name
    try:
        try:
            module = imp(module_name, package=package, **kwargs)
        except ValueError as exc:
            reraise(ValueError,
                    ValueError("Couldn't import {0!r}: {1}".format(name, exc)),
                    sys.exc_info()[2])
        return getattr(module, cls_name) if cls_name else module
    except (ImportError, AttributeError):
        if default is None:
            raise
    return default


class WhateverIO(StringIO):

    def __init__(self, v=None, *a, **kw):
        _SIO_init(self, v.decode() if isinstance(v, bytes) else v, *a, **kw)

    def write(self, data):
        _SIO_write(self, data.decode() if isinstance(data, bytes) else data)


def noop(*args, **kwargs):
    pass


class Mock(mock.Mock):

    def __init__(self, *args, **kwargs):
        attrs = kwargs.pop('attrs', None) or {}
        super(Mock, self).__init__(*args, **kwargs)
        for attr_name, attr_value in items(attrs):
            setattr(self, attr_name, attr_value)


class _ContextMock(Mock):
    """Dummy class implementing __enter__ and __exit__
    as the :keyword:`with` statement requires these to be implemented
    in the class, not just the instance."""

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        pass


def ContextMock(*args, **kwargs):
    obj = _ContextMock(*args, **kwargs)
    obj.attach_mock(_ContextMock(), '__enter__')
    obj.attach_mock(_ContextMock(), '__exit__')
    obj.__enter__.return_value = obj
    # if __exit__ return a value the exception is ignored,
    # so it must return None here.
    obj.__exit__.return_value = None
    return obj


def _bind(f, o):
    @wraps(f)
    def bound_meth(*fargs, **fkwargs):
        return f(o, *fargs, **fkwargs)
    return bound_meth


if PY3:  # pragma: no cover
    def _get_class_fun(meth):
        return meth
else:
    def _get_class_fun(meth):
        return meth.__func__


class MockCallbacks(object):

    def __new__(cls, *args, **kwargs):
        r = Mock(name=cls.__name__)
        _get_class_fun(cls.__init__)(r, *args, **kwargs)
        for key, value in items(vars(cls)):
            if key not in ('__dict__', '__weakref__', '__new__', '__init__'):
                if inspect.ismethod(value) or inspect.isfunction(value):
                    r.__getattr__(key).side_effect = _bind(value, r)
                else:
                    r.__setattr__(key, value)
        return r


# -- adds assertWarns from recent unittest2, not in Python 2.7.

class _AssertRaisesBaseContext(object):

    def __init__(self, expected, test_case, callable_obj=None,
                 expected_regex=None):
        self.expected = expected
        self.failureException = test_case.failureException
        self.obj_name = None
        if isinstance(expected_regex, string_types):
            expected_regex = re.compile(expected_regex)
        self.expected_regex = expected_regex


def _is_magic_module(m):
    # some libraries create custom module types that are lazily
    # lodaded, e.g. Django installs some modules in sys.modules that
    # will load _tkinter and other shit when touched.

    # pyflakes refuses to accept 'noqa' for this isinstance.
    cls, modtype = type(m), types.ModuleType
    try:
        variables = vars(cls)
    except TypeError:
        return True
    else:
        return (cls is not modtype and (
            '__getattr__' in variables or
            '__getattribute__' in variables))


class _AssertWarnsContext(_AssertRaisesBaseContext):
    """A context manager used to implement TestCase.assertWarns* methods."""

    def __enter__(self):
        # The __warningregistry__'s need to be in a pristine state for tests
        # to work properly.
        warnings.resetwarnings()
        for v in list(values(sys.modules)):
            # do not evaluate Django moved modules and other lazily
            # initialized modules.
            if v and not _is_magic_module(v):
                # use raw __getattribute__ to protect even better from
                # lazily loaded modules
                try:
                    object.__getattribute__(v, '__warningregistry__')
                except AttributeError:
                    pass
                else:
                    object.__setattr__(v, '__warningregistry__', {})
        self.warnings_manager = warnings.catch_warnings(record=True)
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
    DeprecationWarning = DeprecationWarning
    PendingDeprecationWarning = PendingDeprecationWarning

    def patch(self, *path, **options):
        manager = patch('.'.join(path), **options)
        patched = manager.start()
        self.addCleanup(manager.stop)
        return patched

    def mock_modules(self, *mods):
        modules = []
        for mod in mods:
            mod = mod.split('.')
            modules.extend(reversed([
                '.'.join(mod[:-i] if i else mod) for i in range(len(mod))
            ]))
        modules = sorted(set(modules))
        return self.wrap_context(mock_module(*modules))

    def on_nth_call_do(self, mock, side_effect, n=1):

        def on_call(*args, **kwargs):
            if mock.call_count >= n:
                mock.side_effect = side_effect
            return mock.return_value
        mock.side_effect = on_call
        return mock

    def on_nth_call_return(self, mock, retval, n=1):

        def on_call(*args, **kwargs):
            if mock.call_count >= n:
                mock.return_value = retval
            return mock.return_value
        mock.side_effect = on_call
        return mock

    def mask_modules(self, *modules):
        self.wrap_context(mask_modules(*modules))

    def wrap_context(self, context):
        ret = context.__enter__()
        self.addCleanup(partial(context.__exit__, None, None, None))
        return ret

    def mock_environ(self, env_name, env_value):
        return self.wrap_context(mock_environ(env_name, env_value))

    def assertWarns(self, expected_warning):
        return _AssertWarnsContext(expected_warning, self, None)

    def assertWarnsRegex(self, expected_warning, expected_regex):
        return _AssertWarnsContext(expected_warning, self,
                                   None, expected_regex)

    @contextmanager
    def assertDeprecated(self):
        with self.assertWarnsRegex(self.DeprecationWarning,
                                   r'scheduled for removal'):
            yield

    @contextmanager
    def assertPendingDeprecation(self):
        with self.assertWarnsRegex(self.PendingDeprecationWarning,
                                   r'scheduled for deprecation'):
            yield

    def assertDictContainsSubset(self, expected, actual, msg=None):
        missing, mismatched = [], []

        for key, value in items(expected):
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
                'Expected, but missing:\n    %s' % (safe_repr(missing),)
            )
        if unexpected:
            errors.append(
                'Unexpected, but present:\n    %s' % (safe_repr(unexpected),)
            )
        if errors:
            standardMsg = '\n'.join(errors)
            self.fail(self._formatMessage(msg, standardMsg))


class _CallableContext(object):

    def __init__(self, context, cargs, ckwargs, fun):
        self.context = context
        self.cargs = cargs
        self.ckwargs = ckwargs
        self.fun = fun

    def __call__(self, *args, **kwargs):
        return self.fun(*args, **kwargs)

    def __enter__(self):
        self.ctx = self.context(*self.cargs, **self.ckwargs)
        return self.ctx.__enter__()

    def __exit__(self, *einfo):
        if self.ctx:
            return self.ctx.__exit__(*einfo)


def decorator(predicate):

    @wraps(predicate)
    def take_arguments(*pargs, **pkwargs):

        @wraps(predicate)
        def decorator(cls):
            if inspect.isclass(cls):
                orig_setup = cls.setUp
                orig_teardown = cls.tearDown

                @wraps(cls.setUp)
                def around_setup(*args, **kwargs):
                    try:
                        contexts = args[0].__rb3dc_contexts__
                    except AttributeError:
                        contexts = args[0].__rb3dc_contexts__ = []
                    p = predicate(*pargs, **pkwargs)
                    p.__enter__()
                    contexts.append(p)
                    return orig_setup(*args, **kwargs)
                around_setup.__wrapped__ = cls.setUp
                cls.setUp = around_setup

                @wraps(cls.tearDown)
                def around_teardown(*args, **kwargs):
                    try:
                        contexts = args[0].__rb3dc_contexts__
                    except AttributeError:
                        pass
                    else:
                        for context in contexts:
                            context.__exit__(*sys.exc_info())
                    orig_teardown(*args, **kwargs)
                around_teardown.__wrapped__ = cls.tearDown
                cls.tearDown = around_teardown

                return cls
            else:
                @wraps(cls)
                def around_case(*args, **kwargs):
                    with predicate(*pargs, **pkwargs):
                        return cls(*args, **kwargs)
                return around_case

        if len(pargs) == 1 and callable(pargs[0]):
            fun, pargs = pargs[0], ()
            return decorator(fun)
        return _CallableContext(predicate, pargs, pkwargs, decorator)
    return take_arguments


@decorator
@contextmanager
def skip_unless_module(module, name=None):
    try:
        importlib.import_module(module)
    except (ImportError, OSError):
        raise SkipTest('module not installed: {0}'.format(name or module))
    yield


@decorator
@contextmanager
def skip_unless_symbol(symbol, name=None):
    try:
        symbol_by_name(symbol)
    except (AttributeError, ImportError):
        raise SkipTest('missing symbol {0}'.format(name or symbol))
    yield


def get_logger_handlers(logger):
    return [
        h for h in logger.handlers
        if not isinstance(h, logging.NullHandler)
    ]


@decorator
@contextmanager
def wrap_logger(logger, loglevel=logging.ERROR):
    old_handlers = get_logger_handlers(logger)
    sio = WhateverIO()
    siohandler = logging.StreamHandler(sio)
    logger.handlers = [siohandler]

    try:
        yield sio
    finally:
        logger.handlers = old_handlers


@decorator
@contextmanager
def mock_environ(env_name, env_value):
    sentinel = object()
    prev_val = os.environ.get(env_name, sentinel)
    os.environ[env_name] = env_value
    try:
        yield env_value
    finally:
        if prev_val is sentinel:
            os.environ.pop(env_name, None)
        else:
            os.environ[env_name] = prev_val


@decorator
@contextmanager
def sleepdeprived(module=time):
    old_sleep, module.sleep = module.sleep, noop
    try:
        yield
    finally:
        module.sleep = old_sleep


@decorator
@contextmanager
def skip_if_python3(reason='incompatible'):
    if PY3:
        raise SkipTest('Python3: {0}'.format(reason))
    yield


@decorator
@contextmanager
def skip_if_environ(env_var_name):
    if os.environ.get(env_var_name):
        raise SkipTest('envvar {0} set'.format(env_var_name))
    yield


@decorator
@contextmanager
def _skip_test(reason, sign):
    raise SkipTest('{0}: {1}'.format(sign, reason))
    yield
todo = partial(_skip_test, sign='TODO')
skip = partial(_skip_test, sign='SKIP')


# Taken from
# http://bitbucket.org/runeh/snippets/src/tip/missing_modules.py
@decorator
@contextmanager
def mask_modules(*modnames):
    """Ban some modules from being importable inside the context

    For example:

        >>> with mask_modules('sys'):
        ...     try:
        ...         import sys
        ...     except ImportError:
        ...         print('sys not found')
        sys not found

        >>> import sys  # noqa
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


@decorator
@contextmanager
def override_stdouts():
    """Override `sys.stdout` and `sys.stderr` with `WhateverIO`."""
    prev_out, prev_err = sys.stdout, sys.stderr
    prev_rout, prev_rerr = sys.__stdout__, sys.__stderr__
    mystdout, mystderr = WhateverIO(), WhateverIO()
    sys.stdout = sys.__stdout__ = mystdout
    sys.stderr = sys.__stderr__ = mystderr

    try:
        yield mystdout, mystderr
    finally:
        sys.stdout = prev_out
        sys.stderr = prev_err
        sys.__stdout__ = prev_rout
        sys.__stderr__ = prev_rerr


@decorator
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
            setattr(module, name, prev)
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


@decorator
@contextmanager
def sys_platform(value):
    prev, sys.platform = sys.platform, value
    try:
        yield
    finally:
        sys.platform = prev


@decorator
@contextmanager
def reset_modules(*modules):
    prev = {k: sys.modules.pop(k) for k in modules if k in sys.modules}
    try:
        yield
    finally:
        sys.modules.update(prev)


@decorator
@contextmanager
def patch_modules(*modules):
    prev = {}
    for mod in modules:
        prev[mod] = sys.modules.get(mod)
        sys.modules[mod] = types.ModuleType(module_name_t(mod))
    try:
        yield
    finally:
        for name, mod in items(prev):
            if mod is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod


@decorator
@contextmanager
def mock_module(*names):
    prev = {}

    class MockModule(types.ModuleType):

        def __getattr__(self, attr):
            setattr(self, attr, Mock())
            return types.ModuleType.__getattribute__(self, attr)

    mods = []
    for name in names:
        try:
            prev[name] = sys.modules[name]
        except KeyError:
            pass
        mod = sys.modules[name] = MockModule(module_name_t(name))
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
            reraise(x[0], x[1], x[2])
    context.__exit__.side_effect = on_exit
    context.__enter__.return_value = context
    try:
        yield context
    finally:
        context.reset()


@decorator
@contextmanager
def mock_open(typ=WhateverIO, side_effect=None):
    with patch(open_fqdn) as open_:
        with mock_context(open_) as context:
            if side_effect is not None:
                context.__enter__.side_effect = side_effect
            val = context.__enter__.return_value = typ()
            val.__exit__ = Mock()
            yield val


@decorator
@contextmanager
def skip_if_platform(platform_name, name=None):
    if sys.platform.startswith(platform_name):
        raise SkipTest('does not work on {0}'.format(platform_name or name))
    yield
skip_if_jython = partial(skip_if_platform, 'java', name='Jython')
skip_if_win32 = partial(skip_if_platform, 'win32', name='Windows')
skip_if_darwin = partial(skip_if_platform, 'darwin', name='OS X')


@decorator
@contextmanager
def skip_if_pypy():
    if getattr(sys, 'pypy_version_info', None):
        raise SkipTest('does not work on PyPy')
    yield


@decorator
@contextmanager
def restore_logging():
    outs = sys.stdout, sys.stderr, sys.__stdout__, sys.__stderr__
    root = logging.getLogger()
    level = root.level
    handlers = root.handlers

    try:
        yield
    finally:
        sys.stdout, sys.stderr, sys.__stdout__, sys.__stderr__ = outs
        root.level = level
        root.handlers[:] = handlers
