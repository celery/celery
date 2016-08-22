from __future__ import absolute_import, unicode_literals

import pytest

from case import patch

from celery import backends
from celery.backends.amqp import AMQPBackend
from celery.backends.cache import CacheBackend
from celery.exceptions import ImproperlyConfigured


class test_backends:

    @pytest.mark.parametrize('url,expect_cls', [
        ('amqp://', AMQPBackend),
        ('cache+memory://', CacheBackend),
    ])
    def test_get_backend_aliases(self, url, expect_cls, app):
        backend, url = backends.get_backend_by_url(url, app.loader)
        assert isinstance(backend(app=app, url=url), expect_cls)

    def test_unknown_backend(self, app):
        with pytest.raises(ImportError):
            backends.get_backend_cls('fasodaopjeqijwqe', app.loader)

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_default_backend(self, app):
        assert backends.default_backend == app.backend

    def test_backend_by_url(self, app, url='redis://localhost/1'):
        from celery.backends.redis import RedisBackend
        backend, url_ = backends.get_backend_by_url(url, app.loader)
        assert backend is RedisBackend
        assert url_ == url

    def test_sym_raises_ValuError(self, app):
        with patch('celery.backends.symbol_by_name') as sbn:
            sbn.side_effect = ValueError()
            with pytest.raises(ImproperlyConfigured):
                backends.get_backend_cls('xxx.xxx:foo', app.loader)
