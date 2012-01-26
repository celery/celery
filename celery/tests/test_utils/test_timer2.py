from __future__ import absolute_import
from __future__ import with_statement

import sys
import time

from kombu.tests.utils import redirect_stdouts
from mock import Mock, patch

import celery.utils.timer2 as timer2

from celery.tests.utils import Case, skip_if_quick


class test_Entry(Case):

    def test_call(self):
        scratch = [None]

        def timed(x, y, moo="foo"):
            scratch[0] = (x, y, moo)

        tref = timer2.Entry(timed, (4, 4), {"moo": "baz"})
        tref()

        self.assertTupleEqual(scratch[0], (4, 4, "baz"))

    def test_cancel(self):
        tref = timer2.Entry(lambda x: x, (1, ), {})
        tref.cancel()
        self.assertTrue(tref.cancelled)


class test_Schedule(Case):

    def test_handle_error(self):
        from datetime import datetime
        mktime = timer2.mktime
        scratch = [None]

        def _overflow(x):
            raise OverflowError(x)

        def on_error(exc_info):
            scratch[0] = exc_info

        s = timer2.Schedule(on_error=on_error)

        timer2.mktime = _overflow
        try:
            s.enter(timer2.Entry(lambda: None, (), {}),
                    eta=datetime.now())
            s.enter(timer2.Entry(lambda: None, (), {}),
                    eta=None)
            s.on_error = None
            with self.assertRaises(OverflowError):
                s.enter(timer2.Entry(lambda: None, (), {}),
                        eta=datetime.now())
        finally:
            timer2.mktime = mktime

        _, exc, _ = scratch[0]
        self.assertIsInstance(exc, OverflowError)


class test_Timer(Case):

    @skip_if_quick
    def test_enter_after(self):
        t = timer2.Timer()
        done = [False]

        def set_done():
            done[0] = True

        try:
            t.apply_after(300, set_done)
            while not done[0]:
                time.sleep(0.1)
        finally:
            t.stop()

    def test_exit_after(self):
        t = timer2.Timer()
        t.apply_after = Mock()
        t.exit_after(300, priority=10)
        t.apply_after.assert_called_with(300, sys.exit, 10)

    def test_apply_interval(self):
        t = timer2.Timer()
        t.enter_after = Mock()

        myfun = Mock()
        t.apply_interval(30, myfun)

        self.assertEqual(t.enter_after.call_count, 1)
        args1, _ = t.enter_after.call_args_list[0]
        msec1, tref1, _ = args1
        self.assertEqual(msec1, 30)
        tref1()

        self.assertEqual(t.enter_after.call_count, 2)
        args2, _ = t.enter_after.call_args_list[1]
        msec2, tref2, _ = args2
        self.assertEqual(msec2, 30)
        tref2.cancelled = True
        tref2()

        self.assertEqual(t.enter_after.call_count, 2)

    @redirect_stdouts
    def test_apply_entry_error_handled(self, stdout, stderr):
        t = timer2.Timer()
        t.schedule.on_error = None

        fun = Mock()
        fun.side_effect = ValueError()

        with self.assertWarns(timer2.TimedFunctionFailed):
            t.apply_entry(fun)
            fun.assert_called_with()

    @redirect_stdouts
    def test_apply_entry_error_not_handled(self, stdout, stderr):
        t = timer2.Timer()
        t.schedule.on_error = Mock()

        fun = Mock()
        fun.side_effect = ValueError()
        t.apply_entry(fun)
        fun.assert_called_with()
        self.assertFalse(stderr.getvalue())

    @patch("os._exit")
    def test_thread_crash(self, _exit):
        t = timer2.Timer()
        t._next_entry = Mock()
        t._next_entry.side_effect = OSError(131)
        t.run()
        _exit.assert_called_with(1)

    def test_gc_race_lost(self):
        t = timer2.Timer()
        t._is_stopped.set = Mock()
        t._is_stopped.set.side_effect = TypeError()

        t._is_shutdown.set()
        t.run()
        t._is_stopped.set.assert_called_with()
