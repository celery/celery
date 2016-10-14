"""Fixtures and testing utilities for :pypi:`py.test <pytest>`."""
from __future__ import absolute_import, unicode_literals

import numbers
import os
import pytest
import weakref

from copy import deepcopy
from datetime import datetime, timedelta
from functools import partial

from case import Mock
from case.utils import decorator
from kombu import Queue
from kombu.utils.imports import symbol_by_name

from celery import Celery
from celery.app import current_app
from celery.backends.cache import CacheBackend, DummyClient

# pylint: disable=redefined-outer-name
# Well, they're called fixtures....


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


class Trap(object):
    """Trap that pretends to be an app but raises an exception instead.

    This to protect from code that does not properly pass app instances,
    then falls back to the current_app.
    """

    def __getattr__(self, name):
        raise RuntimeError('Test depends on current_app')


class UnitLogging(symbol_by_name(Celery.log_cls)):
    """Sets up logging for the test application."""

    def __init__(self, *args, **kwargs):
        super(UnitLogging, self).__init__(*args, **kwargs)
        self.already_setup = True


def TestApp(name=None, set_as_current=False, log=UnitLogging,
            broker='memory://', backend=None, **kwargs):
    """App used for testing."""
    test_app = Celery(
        name or 'celery.tests',
        set_as_current=set_as_current,
        log=log, broker=broker,
        backend=backend or 'cache+memory://', **kwargs)
    test_app.add_defaults(deepcopy(CELERY_TEST_CONFIG))
    return test_app


@pytest.fixture(autouse=True)
def app(request):
    """Fixture creating a Celery application instance."""
    from celery import _state
    mark = request.node.get_marker('celery')
    mark = mark and mark.kwargs or {}

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

    test_app = TestApp(set_as_current=False, backend=mark.get('backend'))
    is_not_contained = any([
        not getattr(request.module, 'app_contained', True),
        not getattr(request.cls, 'app_contained', True),
        not getattr(request.function, 'app_contained', True)
    ])
    if is_not_contained:
        test_app.set_current()

    yield test_app

    _state.set_default_app(prev_default_app)
    _state._tls = prev_tls
    _state._tls.current_app = prev_current_app
    if test_app is not prev_current_app:
        test_app.close()
    _state._on_app_finalizers = prev_finalizers
    _state._apps = prev_apps


@pytest.fixture()
def depends_on_current_app(app):
    """Fixture that sets app as current."""
    app.set_current()


@pytest.fixture(autouse=True)
def reset_cache_backend_state(app):
    """Fixture that resets the internal state of the cache result backend."""
    yield
    backend = app.__dict__.get('backend')
    if backend is not None:
        if isinstance(backend, CacheBackend):
            if isinstance(backend.client, DummyClient):
                backend.client.cache.clear()
            backend._cache.clear()


@decorator
def assert_signal_called(signal, **expected):
    """Context that verifes signal is called before exiting."""
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
    """Create task message in protocol 2 format."""
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
    """Create task message in protocol 1 format."""
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
    """Create task message from :class:`celery.Signature`."""
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
