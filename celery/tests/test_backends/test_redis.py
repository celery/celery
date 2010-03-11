import sys
import errno
import socket
import unittest

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


def get_redis_or_None():

    def emit_no_redis_msg(reason):
        global _no_redis_msg_emitted
        if not _no_redis_msg_emitted:
            sys.stderr.write("\n" + _no_redis_msg % reason + "\n")
            _no_redis_msg_emitted = True

    if pyredis.redis is None:
        return emit_no_redis_msg("not installed")
    try:
        tb = RedisBackend(redis_db="celery_unittest")
        try:
            tb.open()
            # Evaluate lazy connection
            tb._connection.connection.connect(tb._connection)
        except ConnectionError, exc:
            return emit_no_redis_msg("not running")
        return tb
    except ImproperlyConfigured, exc:
        if "need to install" in str(exc):
            return emit_no_redis_msg("not installed")
        return emit_no_redis_msg("not configured")


class TestRedisBackend(unittest.TestCase):

    def test_cached_connection(self):
        tb = get_redis_or_None()
        if not tb:
            return # Skip test

        self.assertTrue(tb._connection is not None)
        tb.close()
        self.assertTrue(tb._connection is None)
        tb.open()
        self.assertTrue(tb._connection is not None)

    def test_mark_as_done(self):
        tb = get_redis_or_None()
        if not tb:
            return

        tid = gen_unique_id()

        self.assertFalse(tb.is_successful(tid))
        self.assertEqual(tb.get_status(tid), states.PENDING)
        self.assertEqual(tb.get_result(tid), None)

        tb.mark_as_done(tid, 42)
        self.assertTrue(tb.is_successful(tid))
        self.assertEqual(tb.get_status(tid), states.SUCCESS)
        self.assertEqual(tb.get_result(tid), 42)
        self.assertTrue(tb.get_result(tid), 42)

    def test_is_pickled(self):
        tb = get_redis_or_None()
        if not tb:
            return

        tid2 = gen_unique_id()
        result = {"foo": "baz", "bar": SomeClass(12345)}
        tb.mark_as_done(tid2, result)
        # is serialized properly.
        rindb = tb.get_result(tid2)
        self.assertEqual(rindb.get("foo"), "baz")
        self.assertEqual(rindb.get("bar").data, 12345)

    def test_mark_as_failure(self):
        tb = get_redis_or_None()
        if not tb:
            return

        tid3 = gen_unique_id()
        try:
            raise KeyError("foo")
        except KeyError, exception:
            pass
        tb.mark_as_failure(tid3, exception)
        self.assertFalse(tb.is_successful(tid3))
        self.assertEqual(tb.get_status(tid3), states.FAILURE)
        self.assertTrue(isinstance(tb.get_result(tid3), KeyError))

    def test_process_cleanup(self):
        tb = get_redis_or_None()
        if not tb:
            return

        tb.process_cleanup()

        self.assertTrue(tb._connection is None)

    def test_connection_close_if_connected(self):
        tb = get_redis_or_None()
        if not tb:
            return

        tb.open()
        self.assertTrue(tb._connection is not None)
        tb.close()
        self.assertTrue(tb._connection is None)
        tb.close()
        self.assertTrue(tb._connection is None)


class TestTyrantBackendNoTyrant(unittest.TestCase):

    def test_tyrant_None_if_tyrant_not_installed(self):
        prev = sys.modules.pop("celery.backends.pyredis")
        try:
            def with_redis_masked(_val):
                from celery.backends.pyredis import redis
                self.assertTrue(redis is None)
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

    def test_constructor_raises_if_not_host_or_port(self):
        from celery.backends import pyredis
        prev_host = pyredis.RedisBackend.redis_host
        prev_port = pyredis.RedisBackend.redis_port
        pyredis.RedisBackend.redis_host = None
        pyredis.RedisBackend.redis_port = None
        try:
            self.assertRaises(ImproperlyConfigured, pyredis.RedisBackend)
        finally:
            pyredis.RedisBackend.redis_host = prev_host
            pyredis.RedisBackend.redis_port = prev_port
