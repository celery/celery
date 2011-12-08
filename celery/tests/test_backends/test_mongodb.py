from __future__ import absolute_import

import sys

from nose import SkipTest

from celery.backends.mongodb import MongoBackend
from celery.exceptions import ImproperlyConfigured
from celery.tests.utils import unittest
from celery.utils import uuid


_no_mongo_msg = "* MongoDB %s. Will not execute related tests."
_no_mongo_msg_emitted = False


try:
    from pymongo.errors import AutoReconnect
except ImportError:

    class AutoReconnect(Exception):  # noqa
        pass


def get_mongo_or_SkipTest():

    def emit_no_mongo_msg(reason):
        global _no_mongo_msg_emitted
        if not _no_mongo_msg_emitted:
            sys.stderr.write("\n" + _no_mongo_msg % reason + "\n")
            _no_mongo_msg_emitted = True

    try:
        tb = MongoBackend()
        try:
            tb._get_database()
        except AutoReconnect, exc:
            emit_no_mongo_msg("not running")
            raise SkipTest("Can't connect to MongoDB: %s" % (exc, ))
        return tb
    except ImproperlyConfigured, exc:
        if "need to install" in str(exc):
            emit_no_mongo_msg("pymongo not installed")
            raise SkipTest("pymongo not installed")
        emit_no_mongo_msg("not configured")
        raise SkipTest("MongoDB not configured correctly: %s" % (exc, ))


class TestMongoBackend(unittest.TestCase):

    def test_save__restore__delete_taskset(self):
        tb = get_mongo_or_SkipTest()

        tid = uuid()
        res = {u"foo": "bar"}
        self.assertEqual(tb.save_taskset(tid, res), res)

        res2 = tb.restore_taskset(tid)
        self.assertEqual(res2, res)

        tb.delete_taskset(tid)
        self.assertIsNone(tb.restore_taskset(tid))

        self.assertIsNone(tb.restore_taskset("xxx-nonexisting-id"))
