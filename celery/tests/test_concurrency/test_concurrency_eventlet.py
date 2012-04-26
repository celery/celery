from __future__ import absolute_import

import os
import sys

from nose import SkipTest
from mock import patch, Mock

from celery.concurrency.eventlet import (
    apply_target,
    Schedule,
    Timer,
    TaskPool,
)

from celery.tests.utils import Case, mock_module


class EventletCase(Case):

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
            import celery.concurrency.eventlet  # noqa
            self.assertTrue(monkey_patched)
        finally:
            sys.modules["celery.concurrency.eventlet"] = prev_eventlet
            os.environ["EVENTLET_NOPATCH"] = "yes"
            self.eventlet.monkey_patch = prev_monkey_patch


eventlet_modules = (
    "eventlet",
    "eventlet.debug",
    "eventlet.greenthread",
    "eventlet.greenpool",
    "greenlet",
)


class test_Schedule(Case):

    def test_sched(self):
        with mock_module(*eventlet_modules):
            @patch("eventlet.greenthread.spawn_after")
            @patch("greenlet.GreenletExit")
            def do_test(GreenletExit, spawn_after):
                x = Schedule()
                x.GreenletExit = KeyError
                entry = Mock()
                g = x._enter(1, 0, entry)
                self.assertTrue(x.queue)

                x._entry_exit(g, entry)
                g.wait.side_effect = KeyError()
                x._entry_exit(g, entry)
                entry.cancel.assert_called_with()
                self.assertFalse(x._queue)

                x._queue.add(g)
                x.clear()
                x._queue.add(g)
                g.cancel.side_effect = KeyError()
                x.clear()

            do_test()


class test_TasKPool(Case):

    def test_pool(self):
        with mock_module(*eventlet_modules):
            @patch("eventlet.greenpool.GreenPool")
            @patch("eventlet.greenthread")
            def do_test(greenthread, GreenPool):
                x = TaskPool()
                x.on_start()
                x.on_stop()
                x.on_apply(Mock())
                x._pool = None
                x.on_stop()
                self.assertTrue(x.getpid())

            do_test()

    @patch("celery.concurrency.eventlet.base")
    def test_apply_target(self, base):
        apply_target(Mock(), getpid=Mock())
        self.assertTrue(base.apply_target.called)


class test_Timer(Case):

    def test_timer(self):
        x = Timer()
        x.ensure_started()
        x.schedule = Mock()
        x.start()
        x.stop()
        x.schedule.clear.assert_called_with()

        tref = Mock()
        x.cancel(tref)
        x.schedule.GreenletExit = KeyError
        tref.cancel.side_effect = KeyError()
        x.cancel(tref)

