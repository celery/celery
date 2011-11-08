from __future__ import absolute_import
from __future__ import with_statement

from celery.app import app_or_default
from celery.events import Events
from celery.events.snapshot import Polaroid, evcam
from celery.tests.utils import unittest


class TRef(object):
    active = True
    called = False

    def __call__(self):
        self.called = True

    def cancel(self):
        self.active = False


class MockTimer(object):
    installed = []

    def apply_interval(self, msecs, fun, *args, **kwargs):
        self.installed.append(fun)
        return TRef()
timer = MockTimer()


class test_Polaroid(unittest.TestCase):

    def setUp(self):
        self.app = app_or_default()
        self.state = self.app.events.State()

    def test_constructor(self):
        x = Polaroid(self.state, app=self.app)
        self.assertIs(x.app, self.app)
        self.assertIs(x.state, self.state)
        self.assertTrue(x.freq)
        self.assertTrue(x.cleanup_freq)
        self.assertTrue(x.logger)
        self.assertFalse(x.maxrate)

    def test_install_timers(self):
        x = Polaroid(self.state, app=self.app)
        x.timer = timer
        x.__exit__()
        x.__enter__()
        self.assertIn(x.capture, MockTimer.installed)
        self.assertIn(x.cleanup, MockTimer.installed)
        self.assertTrue(x._tref.active)
        self.assertTrue(x._ctref.active)
        x.__exit__()
        self.assertFalse(x._tref.active)
        self.assertFalse(x._ctref.active)
        self.assertTrue(x._tref.called)
        self.assertFalse(x._ctref.called)

    def test_cleanup(self):
        x = Polaroid(self.state, app=self.app)
        cleanup_signal_sent = [False]

        def handler(**kwargs):
            cleanup_signal_sent[0] = True

        x.cleanup_signal.connect(handler)
        x.cleanup()
        self.assertTrue(cleanup_signal_sent[0])

    def test_shutter__capture(self):
        x = Polaroid(self.state, app=self.app)
        shutter_signal_sent = [False]

        def handler(**kwargs):
            shutter_signal_sent[0] = True

        x.shutter_signal.connect(handler)
        x.shutter()
        self.assertTrue(shutter_signal_sent[0])

        shutter_signal_sent[0] = False
        x.capture()
        self.assertTrue(shutter_signal_sent[0])

    def test_shutter_maxrate(self):
        x = Polaroid(self.state, app=self.app, maxrate="1/h")
        shutter_signal_sent = [0]

        def handler(**kwargs):
            shutter_signal_sent[0] += 1

        x.shutter_signal.connect(handler)
        for i in range(30):
            x.shutter()
            x.shutter()
            x.shutter()
        self.assertEqual(shutter_signal_sent[0], 1)


class test_evcam(unittest.TestCase):

    class MockReceiver(object):
        raise_keyboard_interrupt = False

        def capture(self, **kwargs):
            if self.__class__.raise_keyboard_interrupt:
                raise KeyboardInterrupt()

    class MockEvents(Events):

        def Receiver(self, *args, **kwargs):
            return test_evcam.MockReceiver()

    def setUp(self):
        self.app = app_or_default()
        self.app.events = self.MockEvents()

    def test_evcam(self):
        evcam(Polaroid, timer=timer)
        evcam(Polaroid, timer=timer, loglevel="CRITICAL")
        self.MockReceiver.raise_keyboard_interrupt = True
        with self.assertRaises(SystemExit):
            evcam(Polaroid, timer=timer)
