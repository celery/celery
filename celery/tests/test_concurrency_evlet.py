import os
import sys

from celery.tests.utils import unittest
from celery.tests.utils import patch


monkey_patched = []


class EventletCase(unittest.TestCase):

    def setUp(self):
        try:
            __import__("eventlet")
        except ImportError:
            raise SkipTest(
                "eventlet not installed, skipping related tests.")



class test_eventlet_patch(EventletCase):

    def setUp(self):
        EventletCase.setUp(self)

    @patch("eventlet", "monkey_patch", lambda: monkey_patched.append(True))
    def test_is_patches(self):
        prev_evlet = sys.modules.pop("celery.concurrency.evlet", None)
        os.environ.pop("EVENTLET_NOPATCH")
        try:
            from celery.concurrency import evlet
            self.assertTrue(evlet)
            self.assertTrue(monkey_patched)
        finally:
            sys.modules["celery.concurrency.evlet"] = prev_evlet
            os.environ["EVENTLET_NOPATCH"] = "yes"
