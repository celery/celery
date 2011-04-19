from celery.tests.utils import unittest

from celery import backends
from celery.backends.amqp import AMQPBackend
from celery.backends.cache import CacheBackend


class TestBackends(unittest.TestCase):

    def test_get_backend_aliases(self):
        from celery import current_app
        print("CACHE BACKEND: %r" % (current_app.conf.CELERY_CACHE_BACKEND, ))
        expects = [("amqp", AMQPBackend),
                   ("cache", CacheBackend)]
        for expect_name, expect_cls in expects:
            self.assertIsInstance(backends.get_backend_cls(expect_name)(),
                                  expect_cls)

    def test_get_backend_cahe(self):
        backends._backend_cache = {}
        backends.get_backend_cls("amqp")
        self.assertIn("amqp", backends._backend_cache)
        amqp_backend = backends.get_backend_cls("amqp")
        self.assertIs(amqp_backend, backends._backend_cache["amqp"])
