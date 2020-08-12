from case import Mock

from celery.worker.heartbeat import Heart


class MockDispatcher:
    heart = None
    next_iter = 0

    def __init__(self):
        self.sent = []
        self.on_enabled = set()
        self.on_disabled = set()
        self.enabled = True

    def send(self, msg, **_fields):
        self.sent.append((msg, _fields))
        if self.heart:
            if self.next_iter > 10:
                self.heart._shutdown.set()
            self.next_iter += 1


class MockTimer:

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
        assert not eventer.sent

    def test_stop_when_disabled(self):
        timer = MockTimer()
        eventer = MockDispatcher()
        eventer.enabled = False
        h = Heart(timer, eventer)
        h.stop()
        assert not eventer.sent

    def test_message_retries(self):
        timer = MockTimer()
        eventer = MockDispatcher()
        eventer.enabled = True
        h = Heart(timer, eventer, interval=1)

        h.start()
        assert eventer.sent[-1][0] == "worker-online"

        # Invoke a heartbeat
        h.tref[1](*h.tref[2], **h.tref[3])
        assert eventer.sent[-1][0] == "worker-heartbeat"
        assert eventer.sent[-1][1]["retry"]

        h.stop()
        assert eventer.sent[-1][0] == "worker-offline"
        assert not eventer.sent[-1][1]["retry"]
