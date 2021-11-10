import builtins
import inspect
import io
import logging
import os
import platform
import sys
import threading
import types
import warnings
from contextlib import contextmanager
from functools import wraps
from importlib import import_module, reload
from unittest.mock import MagicMock, Mock, patch

import pytest
from kombu import Queue

from celery.backends.cache import CacheBackend, DummyClient
# we have to import the pytest plugin fixtures here,
# in case user did not do the `python setup.py develop` yet,
# that installs the pytest plugin into the setuptools registry.
from celery.contrib.pytest import (celery_app, celery_enable_logging,
                                   celery_parameters, depends_on_current_app)
from celery.contrib.testing.app import TestApp, Trap
from celery.contrib.testing.mocks import (TaskMessage, TaskMessage1,
                                          task_message_from_sig)

# Tricks flake8 into silencing redefining fixtures warnings.
__all__ = (
    'celery_app', 'celery_enable_logging', 'depends_on_current_app',
    'celery_parameters'
)

try:
    WindowsError = WindowsError
except NameError:

    class WindowsError(Exception):
        pass

PYPY3 = getattr(sys, 'pypy_version_info', None) and sys.version_info[0] > 3

CASE_LOG_REDIRECT_EFFECT = 'Test {0} didn\'t disable LoggingProxy for {1}'
CASE_LOG_LEVEL_EFFECT = 'Test {0} modified the level of the root logger'
CASE_LOG_HANDLER_EFFECT = 'Test {0} modified handlers for the root logger'

_SIO_write = io.StringIO.write
_SIO_init = io.StringIO.__init__

SENTINEL = object()


def noop(*args, **kwargs):
    pass


class WhateverIO(io.StringIO):

    def __init__(self, v=None, *a, **kw):
        _SIO_init(self, v.decode() if isinstance(v, bytes) else v, *a, **kw)

    def write(self, data):
        _SIO_write(self, data.decode() if isinstance(data, bytes) else data)


@pytest.fixture(scope='session')
def celery_config():
    return {
        'broker_url': 'memory://',
        'broker_transport_options': {
            'polling_interval': 0.1
        },
        'result_backend': 'cache+memory://',
        'task_default_queue': 'testcelery',
        'task_default_exchange': 'testcelery',
        'task_default_routing_key': 'testcelery',
        'task_queues': (
            Queue('testcelery', routing_key='testcelery'),
        ),
        'accept_content': ('json', 'pickle'),

        # Mongo results tests (only executed if installed and running)
        'mongodb_backend_settings': {
            'host': os.environ.get('MONGO_HOST') or 'localhost',
            'port': os.environ.get('MONGO_PORT') or 27017,
            'database': os.environ.get('MONGO_DB') or 'celery_unittests',
            'taskmeta_collection': (
                os.environ.get('MONGO_TASKMETA_COLLECTION') or
                'taskmeta_collection'
            ),
            'user': os.environ.get('MONGO_USER'),
            'password': os.environ.get('MONGO_PASSWORD'),
        }
    }


@pytest.fixture(scope='session')
def use_celery_app_trap():
    return True


@pytest.fixture(autouse=True)
def reset_cache_backend_state(celery_app):
    """Fixture that resets the internal state of the cache result backend."""
    yield
    backend = celery_app.__dict__.get('backend')
    if backend is not None:
        if isinstance(backend, CacheBackend):
            if isinstance(backend.client, DummyClient):
                backend.client.cache.clear()
            backend._cache.clear()


@contextmanager
def assert_signal_called(signal, **expected):
    """Context that verifes signal is called before exiting."""
    handler = Mock()

    def on_call(**kwargs):
        return handler(**kwargs)

    signal.connect(on_call)
    try:
        yield handler
    finally:
        signal.disconnect(on_call)
    handler.assert_called_with(signal=signal, **expected)


@pytest.fixture
def app(celery_app):
    yield celery_app


@pytest.fixture(autouse=True, scope='session')
def AAA_disable_multiprocessing():
    # pytest-cov breaks if a multiprocessing.Process is started,
    # so disable them completely to make sure it doesn't happen.
    stuff = [
        'multiprocessing.Process',
        'billiard.Process',
        'billiard.context.Process',
        'billiard.process.Process',
        'billiard.process.BaseProcess',
        'multiprocessing.Process',
    ]
    ctxs = [patch(s) for s in stuff]
    [ctx.__enter__() for ctx in ctxs]

    yield

    [ctx.__exit__(*sys.exc_info()) for ctx in ctxs]


def alive_threads():
    return [
        thread
        for thread in threading.enumerate()
        if not thread.name.startswith("pytest_timeout ") and thread.is_alive()
    ]


@pytest.fixture(autouse=True)
def task_join_will_not_block():
    from celery import _state, result
    prev_res_join_block = result.task_join_will_block
    _state.orig_task_join_will_block = _state.task_join_will_block
    prev_state_join_block = _state.task_join_will_block
    result.task_join_will_block = \
        _state.task_join_will_block = lambda: False
    _state._set_task_join_will_block(False)

    yield

    result.task_join_will_block = prev_res_join_block
    _state.task_join_will_block = prev_state_join_block
    _state._set_task_join_will_block(False)


@pytest.fixture(scope='session', autouse=True)
def record_threads_at_startup(request):
    try:
        request.session._threads_at_startup
    except AttributeError:
        request.session._threads_at_startup = alive_threads()


@pytest.fixture(autouse=True)
def threads_not_lingering(request):
    yield
    assert request.session._threads_at_startup == alive_threads()


@pytest.fixture(autouse=True)
def AAA_reset_CELERY_LOADER_env():
    yield
    assert not os.environ.get('CELERY_LOADER')


@pytest.fixture(autouse=True)
def test_cases_shortcuts(request, app, patching, celery_config):
    if request.instance:
        @app.task
        def add(x, y):
            return x + y

        # IMPORTANT: We set an .app attribute for every test case class.
        request.instance.app = app
        request.instance.Celery = TestApp
        request.instance.assert_signal_called = assert_signal_called
        request.instance.task_message_from_sig = task_message_from_sig
        request.instance.TaskMessage = TaskMessage
        request.instance.TaskMessage1 = TaskMessage1
        request.instance.CELERY_TEST_CONFIG = celery_config
        request.instance.add = add
        request.instance.patching = patching
    yield
    if request.instance:
        request.instance.app = None


@pytest.fixture(autouse=True)
def sanity_no_shutdown_flags_set():
    yield

    # Make sure no test left the shutdown flags enabled.
    from celery.worker import state as worker_state

    # check for EX_OK
    assert worker_state.should_stop is not False
    assert worker_state.should_terminate is not False
    # check for other true values
    assert not worker_state.should_stop
    assert not worker_state.should_terminate


@pytest.fixture(autouse=True)
def sanity_stdouts(request):
    yield

    from celery.utils.log import LoggingProxy
    assert sys.stdout
    assert sys.stderr
    assert sys.__stdout__
    assert sys.__stderr__
    this = request.node.name
    if isinstance(sys.stdout, (LoggingProxy, Mock)) or \
            isinstance(sys.__stdout__, (LoggingProxy, Mock)):
        raise RuntimeError(CASE_LOG_REDIRECT_EFFECT.format(this, 'stdout'))
    if isinstance(sys.stderr, (LoggingProxy, Mock)) or \
            isinstance(sys.__stderr__, (LoggingProxy, Mock)):
        raise RuntimeError(CASE_LOG_REDIRECT_EFFECT.format(this, 'stderr'))


@pytest.fixture(autouse=True)
def sanity_logging_side_effects(request):
    from _pytest.logging import LogCaptureHandler
    root = logging.getLogger()
    rootlevel = root.level
    roothandlers = [
        x for x in root.handlers if not isinstance(x, LogCaptureHandler)]

    yield

    this = request.node.name
    root_now = logging.getLogger()
    if root_now.level != rootlevel:
        raise RuntimeError(CASE_LOG_LEVEL_EFFECT.format(this))
    newhandlers = [x for x in root_now.handlers if not isinstance(
        x, LogCaptureHandler)]
    if newhandlers != roothandlers:
        raise RuntimeError(CASE_LOG_HANDLER_EFFECT.format(this))


def setup_session(scope='session'):
    using_coverage = (
        os.environ.get('COVER_ALL_MODULES') or '--with-coverage' in sys.argv
    )
    os.environ.update(
        # warn if config module not found
        C_WNOCONF='yes',
        KOMBU_DISABLE_LIMIT_PROTECTION='yes',
    )

    if using_coverage and not PYPY3:
        from warnings import catch_warnings
        with catch_warnings(record=True):
            import_all_modules()
        warnings.resetwarnings()
    from celery._state import set_default_app
    set_default_app(Trap())


def teardown():
    # Don't want SUBDEBUG log messages at finalization.
    try:
        from multiprocessing.util import get_logger
    except ImportError:
        pass
    else:
        get_logger().setLevel(logging.WARNING)

    # Make sure test database is removed.
    import os
    if os.path.exists('test.db'):
        try:
            os.remove('test.db')
        except OSError:
            pass

    # Make sure there are no remaining threads at shutdown.
    import threading
    remaining_threads = [thread for thread in threading.enumerate()
                         if thread.getName() != 'MainThread']
    if remaining_threads:
        sys.stderr.write(
            '\n\n**WARNING**: Remaining threads at teardown: %r...\n' % (
                remaining_threads))


def find_distribution_modules(name=__name__, file=__file__):
    current_dist_depth = len(name.split('.')) - 1
    current_dist = os.path.join(os.path.dirname(file),
                                *([os.pardir] * current_dist_depth))
    abs = os.path.abspath(current_dist)
    dist_name = os.path.basename(abs)

    for dirpath, dirnames, filenames in os.walk(abs):
        package = (dist_name + dirpath[len(abs):]).replace('/', '.')
        if '__init__.py' in filenames:
            yield package
            for filename in filenames:
                if filename.endswith('.py') and filename != '__init__.py':
                    yield '.'.join([package, filename])[:-3]


def import_all_modules(name=__name__, file=__file__,
                       skip=('celery.decorators',
                             'celery.task')):
    for module in find_distribution_modules(name, file):
        if not module.startswith(skip):
            try:
                import_module(module)
            except ImportError:
                pass
            except OSError as exc:
                warnings.warn(UserWarning(
                    'Ignored error importing module {}: {!r}'.format(
                        module, exc,
                    )))


@pytest.fixture
def sleepdeprived(request):
    """Mock sleep method in patched module to do nothing.

    Example:
        >>> import time
        >>> @pytest.mark.sleepdeprived_patched_module(time)
        >>> def test_foo(self, sleepdeprived):
        >>>     pass
    """
    module = request.node.get_closest_marker(
            "sleepdeprived_patched_module").args[0]
    old_sleep, module.sleep = module.sleep, noop
    try:
        yield
    finally:
        module.sleep = old_sleep


# Taken from
# http://bitbucket.org/runeh/snippets/src/tip/missing_modules.py
@pytest.fixture
def mask_modules(request):
    """Ban some modules from being importable inside the context
    For example::
        >>> @pytest.mark.masked_modules('gevent.monkey')
        >>> def test_foo(self, mask_modules):
        ...     try:
        ...         import sys
        ...     except ImportError:
        ...         print('sys not found')
        sys not found
    """
    realimport = builtins.__import__
    modnames = request.node.get_closest_marker("masked_modules").args

    def myimp(name, *args, **kwargs):
        if name in modnames:
            raise ImportError('No module named %s' % name)
        else:
            return realimport(name, *args, **kwargs)

    builtins.__import__ = myimp
    try:
        yield
    finally:
        builtins.__import__ = realimport


@pytest.fixture
def environ(request):
    """Mock environment variable value.
    Example::
        >>> @pytest.mark.patched_environ('DJANGO_SETTINGS_MODULE', 'proj.settings')
        >>> def test_other_settings(self, environ):
        ...    ...
    """
    env_name, env_value = request.node.get_closest_marker("patched_environ").args
    prev_val = os.environ.get(env_name, SENTINEL)
    os.environ[env_name] = env_value
    try:
        yield
    finally:
        if prev_val is SENTINEL:
            os.environ.pop(env_name, None)
        else:
            os.environ[env_name] = prev_val


def replace_module_value(module, name, value=None):
    """Mock module value, given a module, attribute name and value.

    Example::

        >>> replace_module_value(module, 'CONSTANT', 3.03)
    """
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


@contextmanager
def platform_pyimp(value=None):
    """Mock :data:`platform.python_implementation`
    Example::
        >>> with platform_pyimp('PyPy'):
        ...     ...
    """
    yield from replace_module_value(platform, 'python_implementation', value)


@contextmanager
def sys_platform(value=None):
    """Mock :data:`sys.platform`

    Example::
        >>> mock.sys_platform('darwin'):
        ...     ...
    """
    prev, sys.platform = sys.platform, value
    try:
        yield
    finally:
        sys.platform = prev


@contextmanager
def pypy_version(value=None):
    """Mock :data:`sys.pypy_version_info`

    Example::
        >>> with pypy_version((3, 6, 1)):
        ...     ...
    """
    yield from replace_module_value(sys, 'pypy_version_info', value)


def _restore_logging():
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


@contextmanager
def restore_logging_context_manager():
    """Restore root logger handlers after test returns.
    Example::
        >>> with restore_logging_context_manager():
        ...     setup_logging()
    """
    yield from _restore_logging()


@pytest.fixture
def restore_logging(request):
    """Restore root logger handlers after test returns.
    Example::
        >>> def test_foo(self, restore_logging):
        ...     setup_logging()
    """
    yield from _restore_logging()


@pytest.fixture
def module(request):
    """Mock one or modules such that every attribute is a :class:`Mock`."""
    yield from _module(*request.node.get_closest_marker("patched_module").args)


@contextmanager
def module_context_manager(*names):
    """Mock one or modules such that every attribute is a :class:`Mock`."""
    yield from _module(*names)


def _module(*names):
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


class _patching:

    def __init__(self, monkeypatch, request):
        self.monkeypatch = monkeypatch
        self.request = request

    def __getattr__(self, name):
        return getattr(self.monkeypatch, name)

    def __call__(self, path, value=SENTINEL, name=None,
                 new=MagicMock, **kwargs):
        value = self._value_or_mock(value, new, name, path, **kwargs)
        self.monkeypatch.setattr(path, value)
        return value

    def object(self, target, attribute, *args, **kwargs):
        return _wrap_context(
            patch.object(target, attribute, *args, **kwargs),
            self.request)

    def _value_or_mock(self, value, new, name, path, **kwargs):
        if value is SENTINEL:
            value = new(name=name or path.rpartition('.')[2])
        for k, v in kwargs.items():
            setattr(value, k, v)
        return value

    def setattr(self, target, name=SENTINEL, value=SENTINEL, **kwargs):
        # alias to __call__ with the interface of pytest.monkeypatch.setattr
        if value is SENTINEL:
            value, name = name, None
        return self(target, value, name=name)

    def setitem(self, dic, name, value=SENTINEL, new=MagicMock, **kwargs):
        # same as pytest.monkeypatch.setattr but default value is MagicMock
        value = self._value_or_mock(value, new, name, dic, **kwargs)
        self.monkeypatch.setitem(dic, name, value)
        return value

    def modules(self, *mods):
        modules = []
        for mod in mods:
            mod = mod.split('.')
            modules.extend(reversed([
                '.'.join(mod[:-i] if i else mod) for i in range(len(mod))
            ]))
        modules = sorted(set(modules))
        return _wrap_context(module_context_manager(*modules), self.request)


def _wrap_context(context, request):
    ret = context.__enter__()

    def fin():
        context.__exit__(*sys.exc_info())
    request.addfinalizer(fin)
    return ret


@pytest.fixture()
def patching(monkeypatch, request):
    """Monkeypath.setattr shortcut.
    Example:
        .. code-block:: python
        >>> def test_foo(patching):
        >>>     # execv value here will be mock.MagicMock by default.
        >>>     execv = patching('os.execv')
        >>>     patching('sys.platform', 'darwin')  # set concrete value
        >>>     patching.setenv('DJANGO_SETTINGS_MODULE', 'x.settings')
        >>>     # val will be of type mock.MagicMock by default
        >>>     val = patching.setitem('path.to.dict', 'KEY')
    """
    return _patching(monkeypatch, request)


@contextmanager
def stdouts():
    """Override `sys.stdout` and `sys.stderr` with `StringIO`
    instances.
        >>> with conftest.stdouts() as (stdout, stderr):
        ...     something()
        ...     self.assertIn('foo', stdout.getvalue())
    """
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


@contextmanager
def reset_modules(*modules):
    """Remove modules from :data:`sys.modules` by name,
    and reset back again when the test/context returns.
    Example::
        >>> with conftest.reset_modules('celery.result', 'celery.app.base'):
        ...     pass
    """
    prev = {
        k: sys.modules.pop(k) for k in modules if k in sys.modules
    }

    try:
        for k in modules:
            reload(import_module(k))
        yield
    finally:
        sys.modules.update(prev)


def get_logger_handlers(logger):
    return [
        h for h in logger.handlers
        if not isinstance(h, logging.NullHandler)
    ]


@contextmanager
def wrap_logger(logger, loglevel=logging.ERROR):
    """Wrap :class:`logging.Logger` with a StringIO() handler.
    yields a StringIO handle.
    Example::
        >>> with conftest.wrap_logger(logger, loglevel=logging.DEBUG) as sio:
        ...     ...
        ...     sio.getvalue()
    """
    old_handlers = get_logger_handlers(logger)
    sio = WhateverIO()
    siohandler = logging.StreamHandler(sio)
    logger.handlers = [siohandler]

    try:
        yield sio
    finally:
        logger.handlers = old_handlers


@contextmanager
def _mock_context(mock):
    context = mock.return_value = Mock()
    context.__enter__ = Mock()
    context.__exit__ = Mock()

    def on_exit(*x):
        if x[0]:
            raise x[0] from x[1]
    context.__exit__.side_effect = on_exit
    context.__enter__.return_value = context
    try:
        yield context
    finally:
        context.reset()


@contextmanager
def open(side_effect=None):
    """Patch builtins.open so that it returns StringIO object.
    :param side_effect: Additional side effect for when the open context
        is entered.
    Example::
        >>> with mock.open(io.BytesIO) as open_fh:
        ...     something_opening_and_writing_bytes_to_a_file()
        ...     self.assertIn(b'foo', open_fh.getvalue())
    """
    with patch('builtins.open') as open_:
        with _mock_context(open_) as context:
            if side_effect is not None:
                context.__enter__.side_effect = side_effect
            val = context.__enter__.return_value = WhateverIO()
            val.__exit__ = Mock()
            yield val


@contextmanager
def module_exists(*modules):
    """Patch one or more modules to ensure they exist.
    A module name with multiple paths (e.g. gevent.monkey) will
    ensure all parent modules are also patched (``gevent`` +
    ``gevent.monkey``).
    Example::
        >>> with conftest.module_exists('gevent.monkey'):
        ...     gevent.monkey.patch_all = Mock(name='patch_all')
        ...     ...
    """
    gen = []
    old_modules = []
    for module in modules:
        if isinstance(module, str):
            module = types.ModuleType(module)
        gen.append(module)
        if module.__name__ in sys.modules:
            old_modules.append(sys.modules[module.__name__])
        sys.modules[module.__name__] = module
        name = module.__name__
        if '.' in name:
            parent, _, attr = name.rpartition('.')
            setattr(sys.modules[parent], attr, module)
    try:
        yield
    finally:
        for module in gen:
            sys.modules.pop(module.__name__, None)
        for module in old_modules:
            sys.modules[module.__name__] = module


def _bind(f, o):
    @wraps(f)
    def bound_meth(*fargs, **fkwargs):
        return f(o, *fargs, **fkwargs)
    return bound_meth


class MockCallbacks:

    def __new__(cls, *args, **kwargs):
        r = Mock(name=cls.__name__)
        cls.__init__(r, *args, **kwargs)
        for key, value in vars(cls).items():
            if key not in ('__dict__', '__weakref__', '__new__', '__init__'):
                if inspect.ismethod(value) or inspect.isfunction(value):
                    r.__getattr__(key).side_effect = _bind(value, r)
                else:
                    r.__setattr__(key, value)
        return r
