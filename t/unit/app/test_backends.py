import pytest
from case import patch

from celery.app import backends
from celery.backends.amqp import AMQPBackend
from celery.backends.cache import CacheBackend
from celery.exceptions import ImproperlyConfigured


class test_backends:

    @pytest.mark.parametrize('url,expect_cls', [
        ('amqp://', AMQPBackend),
        ('cache+memory://', CacheBackend),
    ])
    def test_get_backend_aliases(self, url, expect_cls, app):
        backend, url = backends.by_url(url, app.loader)
        assert isinstance(backend(app=app, url=url), expect_cls)

    def test_unknown_backend(self, app):
        with pytest.raises(ImportError):
            backends.by_name('fasodaopjeqijwqe', app.loader)

    def test_backend_by_url(self, app, url='redis://localhost/1'):
        from celery.backends.redis import RedisBackend
        backend, url_ = backends.by_url(url, app.loader)
        assert backend is RedisBackend
        assert url_ == url

    def test_sym_raises_ValuError(self, app):
        with patch('celery.app.backends.symbol_by_name') as sbn:
            sbn.side_effect = ValueError()
            with pytest.raises(ImproperlyConfigured):
                backends.by_name('xxx.xxx:foo', app.loader)

    def test_backend_can_not_be_module(self, app):
        with pytest.raises(ImproperlyConfigured):
            backends.by_name(pytest, app.loader)
