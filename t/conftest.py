from __future__ import absolute_import, unicode_literals

import logging
import numbers
import os
import pytest
import sys
import threading
import warnings
import weakref

from copy import deepcopy
from datetime import datetime, timedelta
from functools import partial
from importlib import import_module

from case import Mock
from case.utils import decorator
from kombu import Queue
from kombu.utils.imports import symbol_by_name

from celery import Celery
from celery.app import current_app
from celery.backends.cache import CacheBackend, DummyClient

try:
    WindowsError = WindowsError  # noqa
except NameError:

    class WindowsError(Exception):
        pass

PYPY3 = getattr(sys, 'pypy_version_info', None) and sys.version_info[0] > 3

CASE_LOG_REDIRECT_EFFECT = 'Test {0} didn\'t disable LoggingProxy for {1}'
CASE_LOG_LEVEL_EFFECT = 'Test {0} modified the level of the root logger'
CASE_LOG_HANDLER_EFFECT = 'Test {0} modified handlers for the root logger'

CELERY_TEST_CONFIG = {
    #: Don't want log output when running suite.
    'worker_hijack_root_logger': False,
    'worker_log_color': False,
    'task_default_queue': 'testcelery',
    'task_default_exchange': 'testcelery',
    'task_default_routing_key': 'testcelery',
    'task_queues': (
        Queue('testcelery', routing_key='testcelery'),
    ),
    'accept_content': ('json', 'pickle'),
    'enable_utc': True,
    'timezone': 'UTC',

    # Mongo results tests (only executed if installed and running)
    'mongodb_backend_settings': {
        'host': os.environ.get('MONGO_HOST') or 'localhost',
        'port': os.environ.get('MONGO_PORT') or 27017,
        'database': os.environ.get('MONGO_DB') or 'celery_unittests',
        'taskmeta_collection': (os.environ.get('MONGO_TASKMETA_COLLECTION') or
                                'taskmeta_collection'),
        'user': os.environ.get('MONGO_USER'),
        'password': os.environ.get('MONGO_PASSWORD'),
    }
}


@pytest.fixture(autouse=True, scope='session')
def AAA_disable_multiprocessing(request):
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
    if sys.version_info[0] > 3:
        stuff.append('multiprocessing.process.BaseProcess')
    else:
        stuff.append('multiprocessing.process.Process')
    ctxs = [patch(s) for s in stuff]
    [ctx.__enter__() for ctx in ctxs]

    def fin():
        [ctx.__exit__(*sys.exc_info()) for ctx in ctxs]
    request.addfinalizer(fin)


class Trap(object):

    def __getattr__(self, name):
        raise RuntimeError('Test depends on current_app')


class UnitLogging(symbol_by_name(Celery.log_cls)):

    def __init__(self, *args, **kwargs):
        super(UnitLogging, self).__init__(*args, **kwargs)
        self.already_setup = True


def TestApp(name=None, set_as_current=False, log=UnitLogging,
            broker='memory://', backend='cache+memory://', **kwargs):
    app = Celery(name or 'celery.tests',
                 set_as_current=set_as_current,
                 log=log, broker=broker, backend=backend,
                 **kwargs)
    app.add_defaults(deepcopy(CELERY_TEST_CONFIG))
    return app


def alive_threads():
    return [thread for thread in threading.enumerate() if thread.is_alive()]


@pytest.fixture(autouse=True)
def task_join_will_not_block(request):
    from celery import _state
    from celery import result
    prev_res_join_block = result.task_join_will_block
    _state.orig_task_join_will_block = _state.task_join_will_block
    prev_state_join_block = _state.task_join_will_block
    result.task_join_will_block = \
        _state.task_join_will_block = lambda: False
    _state._set_task_join_will_block(False)

    def fin():
        result.task_join_will_block = prev_res_join_block
        _state.task_join_will_block = prev_state_join_block
        _state._set_task_join_will_block(False)
    request.addfinalizer(fin)


@pytest.fixture(scope='session', autouse=True)
def record_threads_at_startup(request):
    try:
        request.session._threads_at_startup
    except AttributeError:
        request.session._threads_at_startup = alive_threads()


@pytest.fixture(autouse=True)
def threads_not_lingering(request):
    def fin():
        assert request.session._threads_at_startup == alive_threads()
    request.addfinalizer(fin)


@pytest.fixture(autouse=True)
def app(request):
    from celery import _state
    prev_current_app = current_app()
    prev_default_app = _state.default_app
    prev_finalizers = set(_state._on_app_finalizers)
    prev_apps = weakref.WeakSet(_state._apps)
    trap = Trap()
    prev_tls = _state._tls
    _state.set_default_app(trap)

    class NonTLS(object):
        current_app = trap
    _state._tls = NonTLS()

    app = TestApp(set_as_current=False)
    is_not_contained = any([
        not getattr(request.module, 'app_contained', True),
        not getattr(request.cls, 'app_contained', True),
        not getattr(request.function, 'app_contained', True)
    ])
    if is_not_contained:
        app.set_current()

    def fin():
        _state.set_default_app(prev_default_app)
        _state._tls = prev_tls
        _state._tls.current_app = prev_current_app
        if app is not prev_current_app:
            app.close()
        _state._on_app_finalizers = prev_finalizers
        _state._apps = prev_apps
    request.addfinalizer(fin)
    return app


@pytest.fixture()
def depends_on_current_app(app):
    app.set_current()


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

        def fin():
            request.instance.app = None
        request.addfinalizer(fin)


@pytest.fixture(autouse=True)
def zzzz_test_cases_calls_setup_teardown(request):
    if request.instance:
        # we set the .patching attribute for every test class.
        setup = getattr(request.instance, 'setup', None)
        # we also call .setup() and .teardown() after every test method.
        teardown = getattr(request.instance, 'teardown', None)
        setup and setup()
        teardown and request.addfinalizer(teardown)


@pytest.fixture(autouse=True)
def sanity_no_shutdown_flags_set(request):
    def fin():
        # Make sure no test left the shutdown flags enabled.
        from celery.worker import state as worker_state
        # check for EX_OK
        assert worker_state.should_stop is not False
        assert worker_state.should_terminate is not False
        # check for other true values
        assert not worker_state.should_stop
        assert not worker_state.should_terminate
    request.addfinalizer(fin)


@pytest.fixture(autouse=True)
def reset_cache_backend_state(request, app):
    def fin():
        backend = app.__dict__.get('backend')
        if backend is not None:
            if isinstance(backend, CacheBackend):
                if isinstance(backend.client, DummyClient):
                    backend.client.cache.clear()
                backend._cache.clear()
    request.addfinalizer(fin)


@pytest.fixture(autouse=True)
def sanity_stdouts(request):
    def fin():
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
    request.addfinalizer(fin)


@pytest.fixture(autouse=True)
def sanity_logging_side_effects(request):
    root = logging.getLogger()
    rootlevel = root.level
    roothandlers = root.handlers

    def fin():
        this = request.node.name
        root_now = logging.getLogger()
        if root_now.level != rootlevel:
            raise RuntimeError(CASE_LOG_LEVEL_EFFECT.format(this))
        if root_now.handlers != roothandlers:
            raise RuntimeError(CASE_LOG_HANDLER_EFFECT.format(this))
    request.addfinalizer(fin)


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


@decorator
def assert_signal_called(signal, **expected):
    handler = Mock()
    call_handler = partial(handler)
    signal.connect(call_handler)
    try:
        yield handler
    finally:
        signal.disconnect(call_handler)
    handler.assert_called_with(signal=signal, **expected)


def TaskMessage(name, id=None, args=(), kwargs={}, callbacks=None,
                errbacks=None, chain=None, shadow=None, utc=None, **options):
    from celery import uuid
    from kombu.serialization import dumps
    id = id or uuid()
    message = Mock(name='TaskMessage-{0}'.format(id))
    message.headers = {
        'id': id,
        'task': name,
        'shadow': shadow,
    }
    embed = {'callbacks': callbacks, 'errbacks': errbacks, 'chain': chain}
    message.headers.update(options)
    message.content_type, message.content_encoding, message.body = dumps(
        (args, kwargs, embed), serializer='json',
    )
    message.payload = (args, kwargs, embed)
    return message


def TaskMessage1(name, id=None, args=(), kwargs={}, callbacks=None,
                 errbacks=None, chain=None, **options):
    from celery import uuid
    from kombu.serialization import dumps
    id = id or uuid()
    message = Mock(name='TaskMessage-{0}'.format(id))
    message.headers = {}
    message.payload = {
        'task': name,
        'id': id,
        'args': args,
        'kwargs': kwargs,
        'callbacks': callbacks,
        'errbacks': errbacks,
    }
    message.payload.update(options)
    message.content_type, message.content_encoding, message.body = dumps(
        message.payload,
    )
    return message


def task_message_from_sig(app, sig, utc=True, TaskMessage=TaskMessage):
    sig.freeze()
    callbacks = sig.options.pop('link', None)
    errbacks = sig.options.pop('link_error', None)
    countdown = sig.options.pop('countdown', None)
    if countdown:
        eta = app.now() + timedelta(seconds=countdown)
    else:
        eta = sig.options.pop('eta', None)
    if eta and isinstance(eta, datetime):
        eta = eta.isoformat()
    expires = sig.options.pop('expires', None)
    if expires and isinstance(expires, numbers.Real):
        expires = app.now() + timedelta(seconds=expires)
    if expires and isinstance(expires, datetime):
        expires = expires.isoformat()
    return TaskMessage(
        sig.task, id=sig.id, args=sig.args,
        kwargs=sig.kwargs,
        callbacks=[dict(s) for s in callbacks] if callbacks else None,
        errbacks=[dict(s) for s in errbacks] if errbacks else None,
        eta=eta,
        expires=expires,
        utc=utc,
        **sig.options
    )
