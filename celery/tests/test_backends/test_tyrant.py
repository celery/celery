import sys
import errno
import socket
import unittest

from celery.exceptions import ImproperlyConfigured

from celery import states
from celery.utils import gen_unique_id
from celery.backends import tyrant
from celery.backends.tyrant import TyrantBackend

_no_tyrant_msg = "* Tokyo Tyrant %s. Will not execute related tests."
_no_tyrant_msg_emitted = False


class SomeClass(object):

    def __init__(self, data):
        self.data = data


def get_tyrant_or_None():

    def emit_no_tyrant_msg(reason):
        global _no_tyrant_msg_emitted
        if not _no_tyrant_msg_emitted:
            sys.stderr.write("\n" + _no_tyrant_msg % reason + "\n")
            _no_tyrant_msg_emitted = True

    if tyrant.pytyrant is None:
        return emit_no_tyrant_msg("not installed")
    try:
        tb = TyrantBackend()
        try:
            tb.open()
        except socket.error:
            return emit_no_tyrant_msg("not running")
        return tb
    except ImproperlyConfigured, exc:
        if "need to install" in str(exc):
            return emit_no_tyrant_msg("not installed")
        return emit_no_tyrant_msg("not configured")


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
        self.assertEqual(tb.get_status(tid), states.PENDING)
        self.assertEqual(tb.get_result(tid), None)

        tb.mark_as_done(tid, 42)
        self.assertTrue(tb.is_successful(tid))
        self.assertEqual(tb.get_status(tid), states.SUCCESS)
        self.assertEqual(tb.get_result(tid), 42)
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
        self.assertEqual(rindb.get("foo"), "baz")
        self.assertEqual(rindb.get("bar").data, 12345)

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
        self.assertEqual(tb.get_status(tid3), states.FAILURE)
        self.assertTrue(isinstance(tb.get_result(tid3), KeyError))

    def test_process_cleanup(self):
        tb = get_tyrant_or_None()
        if not tb:
            return

        tb.process_cleanup()

        self.assertTrue(tb._connection is None)
