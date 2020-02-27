import socket
import time
import threading

import pytest
from case import patch, skip, Mock
from vine import promise

from celery.backends.asynchronous import BaseResultConsumer
from celery.backends.base import Backend


class DrainerTests(object):
    """
    Base test class for the Default / Gevent / Eventlet drainers.
    """

    interval = 0.1  # Check every tenth of a second

    def get_drainer(self, environment):
        with patch('celery.backends.asynchronous.detect_environment') as d:
            d.return_value = environment
            backend = Backend(self.app)
            consumer = BaseResultConsumer(backend, self.app, backend.accept,
                                          pending_results={},
                                          pending_messages={})
            return consumer.drainer

    @pytest.fixture(autouse=True)
    def setup_drainer(self):
        raise NotImplementedError

    @pytest.fixture(autouse=True)
    def setup_drain_events(self):
        drain_events = self.patching(
            'celery.backends.asynchronous.BaseResultConsumer.drain_events')
        drain_events.side_effect = self.result_consumer_drain_events

    def result_consumer_drain_events(self, timeout=None):
        """
        Subclasses should override this method to define the behavior of
        drainer.result_consumer.drain_events.
        """
        raise NotImplementedError

    def fulfill_promise_after(self, p, after_seconds):
        """
        Subclasses should override this method to fulfill the promise after a number of seconds
        have passed.
        """
        raise NotImplementedError

    def test_drain_backend_checks_on_interval(self):
        p = promise()

        def fulfill_promise_thread():
            # Should run on_interval twice - once (no results) then a second time after the
            # promise is seen to be ready.
            time.sleep(self.interval * 2 - (self.interval / 2))
            p('done')

        threading.Thread(target=fulfill_promise_thread).start()

        on_interval = Mock()
        for _ in self.drainer.drain_events_until(p,
                                                 on_interval=on_interval,
                                                 interval=self.interval):
            pass

        assert p.ready, 'Should have terminated with promise being ready'
        assert on_interval.call_count == 2, 'Should only have called on_interval twice'

    def test_drain_backend_timeout(self):
        p = promise()
        on_interval = Mock()

        # Looking for 6 checks against the on_interval mock
        timeout = (self.interval * 5) + self.interval / 2
        with pytest.raises(socket.timeout):
            for _ in self.drainer.drain_events_until(p,
                                                     on_interval=on_interval,
                                                     interval=self.interval,
                                                     timeout=timeout):
                pass

        assert not p.ready
        assert on_interval.call_count == 6, 'Should have called on_interval six times'


@skip.unless_module('eventlet')
class test_EventletDrainer(DrainerTests):
    @pytest.fixture(autouse=True)
    def setup_drainer(self):
        self.drainer = self.get_drainer('eventlet')

    def fulfill_promise_after(self, p, after_seconds):
        import eventlet
        eventlet.spawn_after(after_seconds, lambda: p('done'))

    def result_consumer_drain_events(self, timeout=None):
        import eventlet
        eventlet.sleep(0)


class test_Drainer(DrainerTests):
    @pytest.fixture(autouse=True)
    def setup_drainer(self):
        self.drainer = self.get_drainer('default')

    def result_consumer_drain_events(self, timeout=None):
        time.sleep(timeout)


@skip.unless_module('gevent')
class test_GeventDrainer(DrainerTests):
    @pytest.fixture(autouse=True)
    def setup_drainer(self):
        self.drainer = self.get_drainer('gevent')

    def result_consumer_drain_events(self, timeout=None):
        import gevent
        gevent.sleep(0)
