"""Fixtures and testing utilities for :pypi:`py.test <pytest>`."""
from __future__ import absolute_import, unicode_literals

import os
import pytest

from contextlib import contextmanager

from celery.backends.cache import CacheBackend, DummyClient

from .testing import worker
from .testing.app import TestApp, setup_default_app_trap

NO_WORKER = os.environ.get('NO_WORKER')

# pylint: disable=redefined-outer-name
# Well, they're called fixtures....


@contextmanager
def _create_app(request, **config):
    test_app = TestApp(set_as_current=False, config=config)
    with setup_default_app_trap(test_app):
        is_not_contained = any([
            not getattr(request.module, 'app_contained', True),
            not getattr(request.cls, 'app_contained', True),
            not getattr(request.function, 'app_contained', True)
        ])
        if is_not_contained:
            test_app.set_current()
        yield test_app


@pytest.fixture(scope='session')
def celery_session_app(request):
    with _create_app(request) as app:
        yield app


@pytest.fixture
def celery_config():
    return {}


@pytest.fixture()
def celery_app(request, celery_config):
    """Fixture creating a Celery application instance."""
    mark = request.node.get_marker('celery')
    config = dict(celery_config, **mark.kwargs if mark else {})
    with _create_app(request, **config) as app:
        yield app


@pytest.fixture()
def celery_worker(request, celery_app):
    if not NO_WORKER:
        worker.start_worker(celery_app)


@pytest.fixture(scope='session')
def celery_session_worker(request, celery_session_app):
    if not NO_WORKER:
        worker.start_worker(celery_session_app)


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
