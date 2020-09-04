from unittest.mock import Mock, patch

import pytest
from case import mock

from celery.app.events import Events
from celery.events.snapshot import Polaroid, evcam


class MockTimer:
    installed = []

    def call_repeatedly(self, secs, fun, *args, **kwargs):
        self.installed.append(fun)
        return Mock(name='TRef')


timer = MockTimer()


class test_Polaroid:

    def setup(self):
        self.state = self.app.events.State()

    def test_constructor(self):
        x = Polaroid(self.state, app=self.app)
        assert x.app is self.app
        assert x.state is self.state
        assert x.freq
        assert x.cleanup_freq
        assert x.logger
        assert not x.maxrate

    def test_install_timers(self):
        x = Polaroid(self.state, app=self.app)
        x.timer = timer
        x.__exit__()
        x.__enter__()
        assert x.capture in MockTimer.installed
        assert x.cleanup in MockTimer.installed
        x._tref.cancel.assert_not_called()
        x._ctref.cancel.assert_not_called()
        x.__exit__()
        x._tref.cancel.assert_called()
        x._ctref.cancel.assert_called()
        x._tref.assert_called()
        x._ctref.assert_not_called()

    def test_cleanup(self):
        x = Polaroid(self.state, app=self.app)
        cleanup_signal_sent = [False]

        def handler(**kwargs):
            cleanup_signal_sent[0] = True

        x.cleanup_signal.connect(handler)
        x.cleanup()
        assert cleanup_signal_sent[0]

    def test_shutter__capture(self):
        x = Polaroid(self.state, app=self.app)
        shutter_signal_sent = [False]

        def handler(**kwargs):
            shutter_signal_sent[0] = True

        x.shutter_signal.connect(handler)
        x.shutter()
        assert shutter_signal_sent[0]

        shutter_signal_sent[0] = False
        x.capture()
        assert shutter_signal_sent[0]

    def test_shutter_maxrate(self):
        x = Polaroid(self.state, app=self.app, maxrate='1/h')
        shutter_signal_sent = [0]

        def handler(**kwargs):
            shutter_signal_sent[0] += 1

        x.shutter_signal.connect(handler)
        for i in range(30):
            x.shutter()
            x.shutter()
            x.shutter()
        assert shutter_signal_sent[0] == 1


class test_evcam:

    class MockReceiver:
        raise_keyboard_interrupt = False

        def capture(self, **kwargs):
            if self.__class__.raise_keyboard_interrupt:
                raise KeyboardInterrupt()

    class MockEvents(Events):

        def Receiver(self, *args, **kwargs):
            return test_evcam.MockReceiver()

    def setup(self):
        self.app.events = self.MockEvents()
        self.app.events.app = self.app

    @mock.restore_logging()
    def test_evcam(self):
        evcam(Polaroid, timer=timer, app=self.app)
        evcam(Polaroid, timer=timer, loglevel='CRITICAL', app=self.app)
        self.MockReceiver.raise_keyboard_interrupt = True
        try:
            with pytest.raises(SystemExit):
                evcam(Polaroid, timer=timer, app=self.app)
        finally:
            self.MockReceiver.raise_keyboard_interrupt = False

    @patch('celery.platforms.create_pidlock')
    def test_evcam_pidfile(self, create_pidlock):
        evcam(Polaroid, timer=timer, pidfile='/var/pid', app=self.app)
        create_pidlock.assert_called_with('/var/pid')
