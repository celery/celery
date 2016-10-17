from __future__ import absolute_import, unicode_literals

import pytest

from cyanide.suite import ManagerMixin


def _celerymark(app, redis_results=None, **kwargs):
    if redis_results and not app.conf.result_backend.startswith('redis'):
        pytest.skip('Test needs Redis result backend.')


@pytest.fixture
def app(request):
    from .app import app
    app.finalize()
    app.set_current()
    mark = request.node.get_marker('celery')
    mark = mark and mark.kwargs or {}
    _celerymark(app, **mark)
    yield app


@pytest.fixture
def manager(app):
    with CeleryManager(app) as manager:
        yield manager


class CeleryManager(ManagerMixin):

    # we don't stop full suite when a task result is missing.
    TaskPredicate = AssertionError

    def __init__(self, app, no_join=False, **kwargs):
        self.app = app
        self.no_join = no_join
        self._init_manager(app, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def close(self):
        pass
