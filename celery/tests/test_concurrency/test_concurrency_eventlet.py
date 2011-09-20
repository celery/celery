from __future__ import absolute_import

import os
import sys

from nose import SkipTest

from celery.tests.utils import unittest


class EventletCase(unittest.TestCase):

    def setUp(self):
        if getattr(sys, "pypy_version_info", None):
            raise SkipTest("Does not work on PyPy")
        try:
            self.eventlet = __import__("eventlet")
        except ImportError:
            raise SkipTest(
                "eventlet not installed, skipping related tests.")


class test_eventlet_patch(EventletCase):

    def test_is_patched(self):
        monkey_patched = []
        prev_monkey_patch = self.eventlet.monkey_patch
        self.eventlet.monkey_patch = lambda: monkey_patched.append(True)
        prev_eventlet = sys.modules.pop("celery.concurrency.eventlet", None)
        os.environ.pop("EVENTLET_NOPATCH")
        try:
            from celery.concurrency import eventlet
            self.assertTrue(eventlet)
            self.assertTrue(monkey_patched)
        finally:
            sys.modules["celery.concurrency.eventlet"] = prev_eventlet
            os.environ["EVENTLET_NOPATCH"] = "yes"
            self.eventlet.monkey_patch = prev_monkey_patch
