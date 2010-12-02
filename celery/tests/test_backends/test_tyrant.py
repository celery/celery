import sys
import socket
from celery.tests.utils import unittest

from nose import SkipTest

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


def get_tyrant_or_SkipTest():

    def emit_no_tyrant_msg(reason):
        global _no_tyrant_msg_emitted
        if not _no_tyrant_msg_emitted:
            sys.stderr.write("\n" + _no_tyrant_msg % reason + "\n")
            _no_tyrant_msg_emitted = True

    if tyrant.pytyrant is None:
        emit_no_tyrant_msg("not installed")
        raise SkipTest("pytyrant library not installed")

    try:
        tb = TyrantBackend()
        try:
            tb.open()
        except socket.error, exc:
            emit_no_tyrant_msg("not running")
            raise SkipTest("Can't connect to Tokyo server: %s" % (exc, ))
        return tb
    except ImproperlyConfigured, exc:
        if "need to install" in str(exc):
            emit_no_tyrant_msg("not installed")
            raise SkipTest("Tokyo Tyrant is not installed")
        emit_no_tyrant_msg("not configured")
        raise SkipTest("Tokyo Tyrant not configured")


class TestTyrantBackend(unittest.TestCase):

    def test_cached_connection(self):
        tb = get_tyrant_or_SkipTest()

        self.assertIsNotNone(tb._connection)
        tb.close()
        self.assertIsNone(tb._connection)
        tb.open()
        self.assertIsNone(tb._connection)

    def test_mark_as_done(self):
        tb = get_tyrant_or_SkipTest()

        tid = gen_unique_id()

        self.assertEqual(tb.get_status(tid), states.PENDING)
        self.assertIsNone(tb.get_result(tid), None)

        tb.mark_as_done(tid, 42)
        self.assertEqual(tb.get_status(tid), states.SUCCESS)
        self.assertEqual(tb.get_result(tid), 42)

    def test_is_pickled(self):
        tb = get_tyrant_or_SkipTest()

        tid2 = gen_unique_id()
        result = {"foo": "baz", "bar": SomeClass(12345)}
        tb.mark_as_done(tid2, result)
        # is serialized properly.
        rindb = tb.get_result(tid2)
        self.assertEqual(rindb.get("foo"), "baz")
        self.assertEqual(rindb.get("bar").data, 12345)

    def test_mark_as_failure(self):
        tb = get_tyrant_or_SkipTest()

        tid3 = gen_unique_id()
        try:
            raise KeyError("foo")
        except KeyError, exception:
            pass
        tb.mark_as_failure(tid3, exception)
        self.assertEqual(tb.get_status(tid3), states.FAILURE)
        self.assertIsInstance(tb.get_result(tid3), KeyError)

    def test_process_cleanup(self):
        tb = get_tyrant_or_SkipTest()

        tb.process_cleanup()

        self.assertIsNone(tb._connection)
