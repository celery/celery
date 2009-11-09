import sys
import unittest
import errno
import socket
from celery.backends import tyrant
from celery.backends.tyrant import Backend as TyrantBackend
from django.conf import settings
from celery.utils import gen_unique_id
from django.core.exceptions import ImproperlyConfigured

_no_tyrant_msg = "* Tokyo Tyrant not running. Will not execute related tests."
_no_tyrant_msg_emitted = False


class SomeClass(object):

    def __init__(self, data):
        self.data = data


def get_tyrant_or_None():

    def emit_no_tyrant_msg():
        global _no_tyrant_msg_emitted
        if not _no_tyrant_msg_emitted:
            sys.stderr.write("\n" + _no_tyrant_msg + "\n")
            _no_tyrant_msg_emitted = True

    if tyrant.pytyrant is None:
        emit_no_tyrant_msg()
        return None
    try:
        tb = TyrantBackend()
        try:
            tb.open()
        except socket.error, exc:
            if exc.errno == errno.ECONNREFUSED:
                emit_no_tyrant_msg()
                return None
            else:
                raise
        return tb
    except ImproperlyConfigured, exc:
        return None


class TestTyrantBackend(unittest.TestCase):

    def test_cached_connection(self):
        tb = get_tyrant_or_None()
        if not tb:
            return # Skip test

        self.assertTrue(tb._connection is not None)
        tb.close()
        self.assertTrue(tb._connection is None)
        tb.open()
        self.assertTrue(tb._connection is not None)

    def test_mark_as_done(self):
        tb = get_tyrant_or_None()
        if not tb:
            return

        tid = gen_unique_id()

        self.assertFalse(tb.is_successful(tid))
        self.assertEquals(tb.get_status(tid), "PENDING")
        self.assertEquals(tb.get_result(tid), None)

        tb.mark_as_done(tid, 42)
        self.assertTrue(tb.is_successful(tid))
        self.assertEquals(tb.get_status(tid), "SUCCESS")
        self.assertEquals(tb.get_result(tid), 42)
        self.assertTrue(tb._cache.get(tid))
        self.assertTrue(tb.get_result(tid), 42)

    def test_is_pickled(self):
        tb = get_tyrant_or_None()
        if not tb:
            return

        tid2 = gen_unique_id()
        result = {"foo": "baz", "bar": SomeClass(12345)}
        tb.mark_as_done(tid2, result)
        # is serialized properly.
        rindb = tb.get_result(tid2)
        self.assertEquals(rindb.get("foo"), "baz")
        self.assertEquals(rindb.get("bar").data, 12345)

    def test_mark_as_failure(self):
        tb = get_tyrant_or_None()
        if not tb:
            return

        tid3 = gen_unique_id()
        try:
            raise KeyError("foo")
        except KeyError, exception:
            pass
        tb.mark_as_failure(tid3, exception)
        self.assertFalse(tb.is_successful(tid3))
        self.assertEquals(tb.get_status(tid3), "FAILURE")
        self.assertTrue(isinstance(tb.get_result(tid3), KeyError))

    def test_process_cleanup(self):
        tb = get_tyrant_or_None()
        if not tb:
            return

        tb.process_cleanup()

        self.assertTrue(tb._connection is None)
