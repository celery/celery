import unittest2 as unittest

from celery import backends
from celery.backends.amqp import AMQPBackend
from celery.backends.pyredis import RedisBackend


class TestBackends(unittest.TestCase):

    def test_get_backend_aliases(self):
        expects = [("amqp", AMQPBackend),
                   ("redis", RedisBackend)]
        for expect_name, expect_cls in expects:
            self.assertIsInstance(backends.get_backend_cls(expect_name)(),
                                  expect_cls)

    def test_get_backend_cahe(self):
        backends._backend_cache = {}
        backends.get_backend_cls("amqp")
        self.assertIn("amqp", backends._backend_cache)
        amqp_backend = backends.get_backend_cls("amqp")
        self.assertIs(amqp_backend, backends._backend_cache["amqp"])
