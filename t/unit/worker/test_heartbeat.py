from __future__ import absolute_import, unicode_literals

from case import Mock

from celery.worker.heartbeat import Heart


class MockDispatcher(object):
    heart = None
    next_iter = 0

    def __init__(self):
        self.sent = []
        self.on_enabled = set()
        self.on_disabled = set()
        self.enabled = True

    def send(self, msg, **_fields):
        self.sent.append(msg)
        if self.heart:
            if self.next_iter > 10:
                self.heart._shutdown.set()
            self.next_iter += 1


class MockTimer(object):

    def call_repeatedly(self, secs, fun, args=(), kwargs={}):

        class entry(tuple):
            canceled = False

            def cancel(self):
                self.canceled = True

        return entry((secs, fun, args, kwargs))

    def cancel(self, entry):
        entry.cancel()


class test_Heart:

    def test_start_stop(self):
        timer = MockTimer()
        eventer = MockDispatcher()
        h = Heart(timer, eventer, interval=1)
        h.start()
        assert h.tref
        h.stop()
        assert h.tref is None
        h.stop()

    def test_send_sends_signal(self):
        h = Heart(MockTimer(), MockDispatcher(), interval=1)
        h._send_sent_signal = None
        h._send('worker-heartbeat')
        h._send_sent_signal = Mock(name='send_sent_signal')
        h._send('worker')
        h._send_sent_signal.assert_called_with(sender=h)

    def test_start_when_disabled(self):
        timer = MockTimer()
        eventer = MockDispatcher()
        eventer.enabled = False
        h = Heart(timer, eventer)
        h.start()
        assert not h.tref

    def test_stop_when_disabled(self):
        timer = MockTimer()
        eventer = MockDispatcher()
        eventer.enabled = False
        h = Heart(timer, eventer)
        h.stop()
