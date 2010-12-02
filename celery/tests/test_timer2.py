import time
from celery.tests.utils import unittest
import celery.utils.timer2 as timer2

from celery.tests.utils import skip_if_quick


class test_Entry(unittest.TestCase):

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


class test_Schedule(unittest.TestCase):

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
        finally:
            timer2.mktime = mktime

        _, exc, _ = scratch[0]
        self.assertIsInstance(exc, OverflowError)


class test_Timer(unittest.TestCase):

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
