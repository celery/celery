from datetime import timedelta

from celery import current_app
from celery import states
from celery.utils import gen_unique_id
from celery.utils.timeutils import timedelta_seconds

from celery.tests.utils import unittest


class Redis(object):

    class Connection(object):
        connected = True

        def disconnect(self):
            self.connected = False

    def __init__(self, host=None, port=None, db=None, password=None, **kw):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.connection = self.Connection()
        self.keyspace = {}
        self.expiry = {}

    def get(self, key):
        return self.keyspace.get(key)

    def set(self, key, value):
        self.keyspace[key] = value

    def expire(self, key, expires):
        self.expiry[key] = expires

    def delete(self, key):
        self.keyspace.pop(key)


class redis(object):
    Redis = Redis


class test_RedisBackend(unittest.TestCase):

    def get_backend(self):
        from celery.backends import pyredis

        class RedisBackend(pyredis.RedisBackend):
            redis = redis

        return RedisBackend

    def setUp(self):
        self.Backend = self.get_backend()

    def test_expires_defaults_to_config(self):
        conf = current_app.conf
        prev = conf.CELERY_TASK_RESULT_EXPIRES
        conf.CELERY_TASK_RESULT_EXPIRES = 10
        try:
            b = self.Backend(expires=None)
            self.assertEqual(b.expires, 10)
        finally:
            conf.CELERY_TASK_RESULT_EXPIRES = prev

    def test_expires_is_int(self):
        b = self.Backend(expires=48)
        self.assertEqual(b.expires, 48)

    def test_expires_is_None(self):
        b = self.Backend(expires=None)
        self.assertEqual(b.expires, timedelta_seconds(
            current_app.conf.CELERY_TASK_RESULT_EXPIRES))

    def test_expires_is_timedelta(self):
        b = self.Backend(expires=timedelta(minutes=1))
        self.assertEqual(b.expires, 60)

    def test_get_set_forget(self):
        b = self.Backend()
        uuid = gen_unique_id()
        b.store_result(uuid, 42, states.SUCCESS)
        self.assertEqual(b.get_status(uuid), states.SUCCESS)
        self.assertEqual(b.get_result(uuid), 42)
        b.forget(uuid)
        self.assertEqual(b.get_status(uuid), states.PENDING)

    def test_set_expires(self):
        b = self.Backend(expires=512)
        uuid = gen_unique_id()
        key = b.get_key_for_task(uuid)
        b.store_result(uuid, 42, states.SUCCESS)
        self.assertEqual(b.client.expiry[key], 512)
