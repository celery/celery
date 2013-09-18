from __future__ import absolute_import

from mock import patch

from celery import backends
from celery.backends.amqp import AMQPBackend
from celery.backends.cache import CacheBackend
from celery.tests.case import AppCase, depends_on_current_app


class test_backends(AppCase):

    def test_get_backend_aliases(self):
        expects = [('amqp://', AMQPBackend),
                   ('cache+memory://', CacheBackend)]

        for url, expect_cls in expects:
            backend, url = backends.get_backend_by_url(url, self.app.loader)
            self.assertIsInstance(
                backend(app=self.app, url=url),
                expect_cls,
            )

    def test_get_backend_cache(self):
        backends.get_backend_cls.clear()
        hits = backends.get_backend_cls.hits
        misses = backends.get_backend_cls.misses
        self.assertTrue(backends.get_backend_cls('amqp', self.app.loader))
        self.assertEqual(backends.get_backend_cls.misses, misses + 1)
        self.assertTrue(backends.get_backend_cls('amqp', self.app.loader))
        self.assertEqual(backends.get_backend_cls.hits, hits + 1)

    def test_unknown_backend(self):
        with self.assertRaises(ImportError):
            backends.get_backend_cls('fasodaopjeqijwqe', self.app.loader)

    @depends_on_current_app
    def test_default_backend(self):
        self.assertEqual(backends.default_backend, self.app.backend)

    def test_backend_by_url(self, url='redis://localhost/1'):
        from celery.backends.redis import RedisBackend
        backend, url_ = backends.get_backend_by_url(url, self.app.loader)
        self.assertIs(backend, RedisBackend)
        self.assertEqual(url_, url)

    def test_sym_raises_ValuError(self):
        with patch('celery.backends.symbol_by_name') as sbn:
            sbn.side_effect = ValueError()
            with self.assertRaises(ValueError):
                backends.get_backend_cls('xxx.xxx:foo', self.app.loader)
