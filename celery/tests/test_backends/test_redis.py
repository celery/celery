from __future__ import absolute_import
from __future__ import with_statement

import sys
import socket

from nose import SkipTest

from celery.exceptions import ImproperlyConfigured

from celery import states
from celery.utils import uuid
from celery.backends import redis
from celery.backends.redis import RedisBackend
from celery.tests.utils import Case, mask_modules

_no_redis_msg = "* Redis %s. Will not execute related tests."
_no_redis_msg_emitted = False

try:
    from redis.exceptions import ConnectionError
except ImportError:
    class ConnectionError(socket.error):  # noqa
        pass


class SomeClass(object):

    def __init__(self, data):
        self.data = data


def get_redis_or_SkipTest():

    def emit_no_redis_msg(reason):
        global _no_redis_msg_emitted
        if not _no_redis_msg_emitted:
            sys.stderr.write("\n" + _no_redis_msg % reason + "\n")
            _no_redis_msg_emitted = True

    if redis.redis is None:
        emit_no_redis_msg("not installed")
        raise SkipTest("redis library not installed")
    try:
        tb = RedisBackend(redis_db="celery_unittest")
        try:
            # Evaluate lazy connection
            tb.client.info()
        except ConnectionError, exc:
            emit_no_redis_msg("not running")
            raise SkipTest("can't connect to redis: %s" % (exc, ))
        return tb
    except ImproperlyConfigured, exc:
        if "need to install" in str(exc):
            return emit_no_redis_msg("not installed")
        return emit_no_redis_msg("not configured")


class TestRedisBackend(Case):

    def test_mark_as_done(self):
        tb = get_redis_or_SkipTest()

        tid = uuid()

        self.assertEqual(tb.get_status(tid), states.PENDING)
        self.assertIsNone(tb.get_result(tid))

        tb.mark_as_done(tid, 42)
        self.assertEqual(tb.get_status(tid), states.SUCCESS)
        self.assertEqual(tb.get_result(tid), 42)

    def test_is_pickled(self):
        tb = get_redis_or_SkipTest()

        tid2 = uuid()
        result = {"foo": "baz", "bar": SomeClass(12345)}
        tb.mark_as_done(tid2, result)
        # is serialized properly.
        rindb = tb.get_result(tid2)
        self.assertEqual(rindb.get("foo"), "baz")
        self.assertEqual(rindb.get("bar").data, 12345)

    def test_mark_as_failure(self):
        tb = get_redis_or_SkipTest()

        tid3 = uuid()
        try:
            raise KeyError("foo")
        except KeyError, exception:
            pass
        tb.mark_as_failure(tid3, exception)
        self.assertEqual(tb.get_status(tid3), states.FAILURE)
        self.assertIsInstance(tb.get_result(tid3), KeyError)


class TestRedisBackendNoRedis(Case):

    def test_redis_None_if_redis_not_installed(self):
        prev = sys.modules.pop("celery.backends.redis")
        try:
            with mask_modules("redis"):
                from celery.backends.redis import redis
                self.assertIsNone(redis)
        finally:
            sys.modules["celery.backends.redis"] = prev

    def test_constructor_raises_if_redis_not_installed(self):
        from celery.backends import redis
        prev = redis.RedisBackend.redis
        redis.RedisBackend.redis = None
        try:
            with self.assertRaises(ImproperlyConfigured):
                redis.RedisBackend()
        finally:
            redis.RedisBackend.redis = prev
