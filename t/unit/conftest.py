from __future__ import absolute_import, unicode_literals

import logging
import os
import sys
import threading
import warnings
from importlib import import_module

import pytest
from case import Mock
from case.utils import decorator
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
    WindowsError = WindowsError  # noqa
except NameError:

    class WindowsError(Exception):
        pass

PYPY3 = getattr(sys, 'pypy_version_info', None) and sys.version_info[0] > 3

CASE_LOG_REDIRECT_EFFECT = 'Test {0} didn\'t disable LoggingProxy for {1}'
CASE_LOG_LEVEL_EFFECT = 'Test {0} modified the level of the root logger'
CASE_LOG_HANDLER_EFFECT = 'Test {0} modified handlers for the root logger'


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


@decorator
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
    from case import patch
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
    return [thread for thread in threading.enumerate() if thread.is_alive()]


@pytest.fixture(autouse=True)
def task_join_will_not_block():
    from celery import _state
    from celery import result
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
        except WindowsError:
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
                    'Ignored error importing module {0}: {1!r}'.format(
                        module, exc,
                    )))
