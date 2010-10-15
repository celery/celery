import sys
import socket
import unittest2 as unittest

from nose import SkipTest

from celery.exceptions import ImproperlyConfigured

from celery import states
from celery.utils import gen_unique_id
from celery.backends import pyredis
from celery.backends.pyredis import RedisBackend

from celery.tests.utils import execute_context, mask_modules

_no_redis_msg = "* Redis %s. Will not execute related tests."
_no_redis_msg_emitted = False

try:
    from redis.exceptions import ConnectionError
except ImportError:
    class ConnectionError(socket.error):
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

    if pyredis.redis is None:
        emit_no_redis_msg("not installed")
        raise SkipTest("redis library not installed")
    try:
        tb = RedisBackend(redis_db="celery_unittest")
        try:
            tb.open()
            # Evaluate lazy connection
            tb._connection.connection.connect(tb._connection)
        except ConnectionError, exc:
            emit_no_redis_msg("not running")
            raise SkipTest("can't connect to redis: %s" % (exc, ))
        return tb
    except ImproperlyConfigured, exc:
        if "need to install" in str(exc):
            return emit_no_redis_msg("not installed")
        return emit_no_redis_msg("not configured")


class TestRedisBackend(unittest.TestCase):

    def test_cached_connection(self):
        tb = get_redis_or_SkipTest()

        self.assertIsNotNone(tb._connection)
        tb.close()
        self.assertIsNone(tb._connection)
        tb.open()
        self.assertIsNotNone(tb._connection)

    def test_mark_as_done(self):
        tb = get_redis_or_SkipTest()

        tid = gen_unique_id()

        self.assertEqual(tb.get_status(tid), states.PENDING)
        self.assertIsNone(tb.get_result(tid))

        tb.mark_as_done(tid, 42)
        self.assertEqual(tb.get_status(tid), states.SUCCESS)
        self.assertEqual(tb.get_result(tid), 42)

    def test_is_pickled(self):
        tb = get_redis_or_SkipTest()

        tid2 = gen_unique_id()
        result = {"foo": "baz", "bar": SomeClass(12345)}
        tb.mark_as_done(tid2, result)
        # is serialized properly.
        rindb = tb.get_result(tid2)
        self.assertEqual(rindb.get("foo"), "baz")
        self.assertEqual(rindb.get("bar").data, 12345)

    def test_mark_as_failure(self):
        tb = get_redis_or_SkipTest()

        tid3 = gen_unique_id()
        try:
            raise KeyError("foo")
        except KeyError, exception:
            pass
        tb.mark_as_failure(tid3, exception)
        self.assertEqual(tb.get_status(tid3), states.FAILURE)
        self.assertIsInstance(tb.get_result(tid3), KeyError)

    def test_process_cleanup(self):
        tb = get_redis_or_SkipTest()

        tb.process_cleanup()

        self.assertIsNone(tb._connection)

    def test_connection_close_if_connected(self):
        tb = get_redis_or_SkipTest()

        tb.open()
        self.assertIsNotNone(tb._connection)
        tb.close()
        self.assertIsNone(tb._connection)
        tb.close()
        self.assertIsNone(tb._connection)


class TestTyrantBackendNoTyrant(unittest.TestCase):

    def test_tyrant_None_if_tyrant_not_installed(self):
        prev = sys.modules.pop("celery.backends.pyredis")
        try:
            def with_redis_masked(_val):
                from celery.backends.pyredis import redis
                self.assertIsNone(redis)
            context = mask_modules("redis")
            execute_context(context, with_redis_masked)
        finally:
            sys.modules["celery.backends.pyredis"] = prev

    def test_constructor_raises_if_tyrant_not_installed(self):
        from celery.backends import pyredis
        prev = pyredis.redis
        pyredis.redis = None
        try:
            self.assertRaises(ImproperlyConfigured, pyredis.RedisBackend)
        finally:
            pyredis.redis = prev
