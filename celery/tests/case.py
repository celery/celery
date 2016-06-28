from __future__ import absolute_import, unicode_literals

import importlib
import inspect
import logging
import numbers
import os
import sys
import threading

from copy import deepcopy
from datetime import datetime, timedelta
from functools import partial

from kombu import Queue
from kombu.utils import symbol_by_name
from vine.utils import wraps

from celery import Celery
from celery.app import current_app
from celery.backends.cache import CacheBackend, DummyClient
from celery.exceptions import CDeprecationWarning, CPendingDeprecationWarning
from celery.utils.imports import qualname

from case import (
    ANY, ContextMock, MagicMock, Mock, call, mock, skip, patch, sentinel,
)
from case import Case as _Case
from case.utils import decorator

__all__ = [
    'ANY', 'ContextMock', 'MagicMock', 'Mock',
    'call', 'mock', 'skip', 'patch', 'sentinel',

    'AppCase', 'TaskMessage', 'TaskMessage1',
    'depends_on_current_app', 'assert_signal_called', 'task_message_from_sig',
]

CASE_REDEFINES_SETUP = """\
{name} (subclass of AppCase) redefines private "setUp", should be: "setup"\
"""
CASE_REDEFINES_TEARDOWN = """\
{name} (subclass of AppCase) redefines private "tearDown", \
should be: "teardown"\
"""
CASE_LOG_REDIRECT_EFFECT = """\
Test {0} did not disable LoggingProxy for {1}\
"""
CASE_LOG_LEVEL_EFFECT = """\
Test {0} Modified the level of the root logger\
"""
CASE_LOG_HANDLER_EFFECT = """\
Test {0} Modified handlers for the root logger\
"""

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


class Case(_Case):
    DeprecationWarning = CDeprecationWarning
    PendingDeprecationWarning = CPendingDeprecationWarning


class Trap(object):

    def __getattr__(self, name):
        raise RuntimeError('Test depends on current_app')


class UnitLogging(symbol_by_name(Celery.log_cls)):

    def __init__(self, *args, **kwargs):
        super(UnitLogging, self).__init__(*args, **kwargs)
        self.already_setup = True


def UnitApp(name=None, set_as_current=False, log=UnitLogging,
            broker='memory://', backend='cache+memory://', **kwargs):
    app = Celery(name or 'celery.tests',
                 set_as_current=set_as_current,
                 log=log, broker=broker, backend=backend,
                 **kwargs)
    app.add_defaults(deepcopy(CELERY_TEST_CONFIG))
    return app


def alive_threads():
    return [thread for thread in threading.enumerate() if thread.is_alive()]


def depends_on_current_app(fun):
    if inspect.isclass(fun):
        fun.contained = False
    else:
        @wraps(fun)
        def __inner(self, *args, **kwargs):
            self.app.set_current()
            return fun(self, *args, **kwargs)
        return __inner


class AppCase(Case):
    contained = True
    _threads_at_startup = [None]

    def __init__(self, *args, **kwargs):
        super(AppCase, self).__init__(*args, **kwargs)
        setUp = self.__class__.__dict__.get('setUp')
        tearDown = self.__class__.__dict__.get('tearDown')
        if setUp and not hasattr(setUp, '__wrapped__'):
            raise RuntimeError(
                CASE_REDEFINES_SETUP.format(name=qualname(self)),
            )
        if tearDown and not hasattr(tearDown, '__wrapped__'):
            raise RuntimeError(
                CASE_REDEFINES_TEARDOWN.format(name=qualname(self)),
            )

    def Celery(self, *args, **kwargs):
        return UnitApp(*args, **kwargs)

    def threads_at_startup(self):
        if self._threads_at_startup[0] is None:
            self._threads_at_startup[0] = alive_threads()
        return self._threads_at_startup[0]

    def setUp(self):
        self._threads_at_setup = self.threads_at_startup()
        from celery import _state
        from celery import result
        self._prev_res_join_block = result.task_join_will_block
        self._prev_state_join_block = _state.task_join_will_block
        result.task_join_will_block = \
            _state.task_join_will_block = lambda: False
        self._current_app = current_app()
        self._default_app = _state.default_app
        trap = Trap()
        self._prev_tls = _state._tls
        _state.set_default_app(trap)

        class NonTLS(object):
            current_app = trap
        _state._tls = NonTLS()

        self.app = self.Celery(set_as_current=False)
        if not self.contained:
            self.app.set_current()
        root = logging.getLogger()
        self.__rootlevel = root.level
        self.__roothandlers = root.handlers
        _state._set_task_join_will_block(False)
        try:
            self.setup()
        except:
            self._teardown_app()
            raise

    def _teardown_app(self):
        from celery import _state
        from celery import result
        from celery.utils.log import LoggingProxy
        assert sys.stdout
        assert sys.stderr
        assert sys.__stdout__
        assert sys.__stderr__
        this = self._get_test_name()
        result.task_join_will_block = self._prev_res_join_block
        _state.task_join_will_block = self._prev_state_join_block
        if isinstance(sys.stdout, (LoggingProxy, Mock)) or \
                isinstance(sys.__stdout__, (LoggingProxy, Mock)):
            raise RuntimeError(CASE_LOG_REDIRECT_EFFECT.format(this, 'stdout'))
        if isinstance(sys.stderr, (LoggingProxy, Mock)) or \
                isinstance(sys.__stderr__, (LoggingProxy, Mock)):
            raise RuntimeError(CASE_LOG_REDIRECT_EFFECT.format(this, 'stderr'))
        backend = self.app.__dict__.get('backend')
        if backend is not None:
            if isinstance(backend, CacheBackend):
                if isinstance(backend.client, DummyClient):
                    backend.client.cache.clear()
                backend._cache.clear()
        from celery import _state
        _state._set_task_join_will_block(False)

        _state.set_default_app(self._default_app)
        _state._tls = self._prev_tls
        _state._tls.current_app = self._current_app
        if self.app is not self._current_app:
            self.app.close()
        self.app = None
        self.assertEqual(self._threads_at_setup, alive_threads())

        # Make sure no test left the shutdown flags enabled.
        from celery.worker import state as worker_state
        # check for EX_OK
        self.assertIsNot(worker_state.should_stop, False)
        self.assertIsNot(worker_state.should_terminate, False)
        # check for other true values
        self.assertFalse(worker_state.should_stop)
        self.assertFalse(worker_state.should_terminate)

    def _get_test_name(self):
        return '.'.join([self.__class__.__name__, self._testMethodName])

    def tearDown(self):
        try:
            self.teardown()
        finally:
            self._teardown_app()
        self.assert_no_logging_side_effect()

    def assert_no_logging_side_effect(self):
        this = self._get_test_name()
        root = logging.getLogger()
        if root.level != self.__rootlevel:
            raise RuntimeError(CASE_LOG_LEVEL_EFFECT.format(this))
        if root.handlers != self.__roothandlers:
            raise RuntimeError(CASE_LOG_HANDLER_EFFECT.format(this))

    def assert_signal_called(self, signal, **expected):
        return assert_signal_called(signal, **expected)

    def setup(self):
        pass

    def teardown(self):
        pass


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


def _old_patch(module, name, mocked):
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
