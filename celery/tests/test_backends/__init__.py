from __future__ import absolute_import
from __future__ import with_statement

from celery import backends
from celery.backends.amqp import AMQPBackend
from celery.backends.cache import CacheBackend
from celery.tests.utils import Case


class TestBackends(Case):

    def test_get_backend_aliases(self):
        expects = [("amqp", AMQPBackend),
                   ("cache", CacheBackend)]
        for expect_name, expect_cls in expects:
            self.assertIsInstance(backends.get_backend_cls(expect_name)(),
                                  expect_cls)

    def test_get_backend_cache(self):
        backends.get_backend_cls.clear()
        hits = backends.get_backend_cls.hits
        misses = backends.get_backend_cls.misses
        self.assertTrue(backends.get_backend_cls("amqp"))
        self.assertEqual(backends.get_backend_cls.misses, misses + 1)
        self.assertTrue(backends.get_backend_cls("amqp"))
        self.assertEqual(backends.get_backend_cls.hits, hits + 1)

    def test_unknown_backend(self):
        with self.assertRaises(ValueError):
            backends.get_backend_cls("fasodaopjeqijwqe")
