from __future__ import absolute_import, unicode_literals

import logging
import os
import pytest
import sys
import threading
import warnings

from importlib import import_module

from case import Mock

from celery.utils.pytest import (
    CELERY_TEST_CONFIG, Trap, TestApp,
    assert_signal_called, TaskMessage, TaskMessage1, task_message_from_sig,
)
from celery.utils.pytest import app  # noqa
from celery.utils.pytest import reset_cache_backend_state  # noqa
from celery.utils.pytest import depends_on_current_app  # noqa

__all__ = ['app', 'reset_cache_backend_state', 'depends_on_current_app']

try:
    WindowsError = WindowsError  # noqa
except NameError:

    class WindowsError(Exception):
        pass

PYPY3 = getattr(sys, 'pypy_version_info', None) and sys.version_info[0] > 3

CASE_LOG_REDIRECT_EFFECT = 'Test {0} didn\'t disable LoggingProxy for {1}'
CASE_LOG_LEVEL_EFFECT = 'Test {0} modified the level of the root logger'
CASE_LOG_HANDLER_EFFECT = 'Test {0} modified handlers for the root logger'


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
def test_cases_shortcuts(request, app, patching):
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
        request.instance.CELERY_TEST_CONFIG = dict(CELERY_TEST_CONFIG)
        request.instance.add = add
        request.instance.patching = patching
    yield
    if request.instance:
        request.instance.app = None


@pytest.fixture(autouse=True)
def zzzz_test_cases_calls_setup_teardown(request):
    if request.instance:
        # we set the .patching attribute for every test class.
        setup = getattr(request.instance, 'setup', None)
        # we also call .setup() and .teardown() after every test method.
        setup and setup()

    yield

    if request.instance:
        teardown = getattr(request.instance, 'teardown', None)
        teardown and teardown()


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
    root = logging.getLogger()
    rootlevel = root.level
    roothandlers = root.handlers

    yield

    this = request.node.name
    root_now = logging.getLogger()
    if root_now.level != rootlevel:
        raise RuntimeError(CASE_LOG_LEVEL_EFFECT.format(this))
    if root_now.handlers != roothandlers:
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
