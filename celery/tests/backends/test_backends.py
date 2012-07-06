from __future__ import absolute_import
from __future__ import with_statement

from mock import patch

from celery import current_app
from celery import backends
from celery.backends.amqp import AMQPBackend
from celery.backends.cache import CacheBackend
from celery.tests.utils import Case


class test_backends(Case):

    def test_get_backend_aliases(self):
        expects = [('amqp', AMQPBackend),
                   ('cache', CacheBackend)]
        for expect_name, expect_cls in expects:
            self.assertIsInstance(backends.get_backend_cls(expect_name)(),
                                  expect_cls)

    def test_get_backend_cache(self):
        backends.get_backend_cls.clear()
        hits = backends.get_backend_cls.hits
        misses = backends.get_backend_cls.misses
        self.assertTrue(backends.get_backend_cls('amqp'))
        self.assertEqual(backends.get_backend_cls.misses, misses + 1)
        self.assertTrue(backends.get_backend_cls('amqp'))
        self.assertEqual(backends.get_backend_cls.hits, hits + 1)

    def test_unknown_backend(self):
        with self.assertRaises(ImportError):
            backends.get_backend_cls('fasodaopjeqijwqe')

    def test_default_backend(self):
        self.assertEqual(backends.default_backend, current_app.backend)

    def test_backend_by_url(self, url='redis://localhost/1'):
        from celery.backends.redis import RedisBackend
        backend, url_ = backends.get_backend_by_url(url)
        self.assertIs(backend, RedisBackend)
        self.assertEqual(url_, url)

    def test_sym_raises_ValuError(self):
        with patch('celery.backends.symbol_by_name') as sbn:
            sbn.side_effect = ValueError()
            with self.assertRaises(ValueError):
                backends.get_backend_cls('xxx.xxx:foo')
