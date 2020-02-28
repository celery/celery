import os
import socket
import time
import threading

import pytest
from case import patch, skip, Mock
from vine import promise

from celery.backends.asynchronous import BaseResultConsumer
from celery.backends.base import Backend


@pytest.fixture(autouse=True)
def setup_eventlet():
    # By default eventlet will patch the DNS resolver when imported.
    os.environ.update(EVENTLET_NO_GREENDNS='yes')


class DrainerTests(object):
    """
    Base test class for the Default / Gevent / Eventlet drainers.
    """

    interval = 0.1  # Check every tenth of a second
    MAX_TIMEOUT = 10  # Specify a max timeout so it doesn't run forever

    def get_drainer(self, environment):
        with patch('celery.backends.asynchronous.detect_environment') as d:
            d.return_value = environment
            backend = Backend(self.app)
            consumer = BaseResultConsumer(backend, self.app, backend.accept,
                                          pending_results={},
                                          pending_messages={})
            consumer.drain_events = Mock(side_effect=self.result_consumer_drain_events)
            return consumer.drainer

    @pytest.fixture(autouse=True)
    def setup_drainer(self):
        raise NotImplementedError

    def result_consumer_drain_events(self, timeout=None):
        """
        Subclasses should override this method to define the behavior of
        drainer.result_consumer.drain_events.
        """
        raise NotImplementedError

    def test_drain_checks_on_interval(self):
        p = promise()

        def fulfill_promise_thread():
            time.sleep(self.interval * 2)
            p('done')

        threading.Thread(target=fulfill_promise_thread).start()

        on_interval = Mock()
        for _ in self.drainer.drain_events_until(p,
                                                 on_interval=on_interval,
                                                 interval=self.interval,
                                                 timeout=self.MAX_TIMEOUT):
            pass

        assert p.ready, 'Should have terminated with promise being ready'
        assert on_interval.call_count < 20, 'Should have limited number of calls to on_interval'

    def test_drain_does_not_block_event_loop(self):
        """
        This test makes sure that other greenlets can still operate while drain_events_until is
        running.
        """
        p = promise()

        def fulfill_promise_thread():
            time.sleep(self.interval * 2)
            p('done')

        liveness_mock = Mock()
        self.schedule_liveness_thread(liveness_mock, p)
        threading.Thread(target=fulfill_promise_thread).start()

        on_interval = Mock()
        for _ in self.drainer.drain_events_until(p,
                                                 on_interval=on_interval,
                                                 interval=self.interval,
                                                 timeout=self.MAX_TIMEOUT):
            pass

        assert p.ready, 'Should have terminated with promise being ready'
        assert on_interval.call_count < liveness_mock.call_count, \
            'Should have served liveness_mock while waiting for event'

    def test_drain_timeout(self):
        p = promise()
        on_interval = Mock()

        with pytest.raises(socket.timeout):
            for _ in self.drainer.drain_events_until(p,
                                                     on_interval=on_interval,
                                                     interval=self.interval,
                                                     timeout=self.interval * 5):
                pass

        assert not p.ready, 'Promise should remain un-fulfilled'
        assert on_interval.call_count < 20, 'Should have limited number of calls to on_interval'


@skip.unless_module('eventlet')
class test_EventletDrainer(DrainerTests):
    @pytest.fixture(autouse=True)
    def setup_drainer(self):
        self.drainer = self.get_drainer('eventlet')

    def result_consumer_drain_events(self, timeout=None):
        import eventlet
        eventlet.sleep(0)

    def schedule_liveness_thread(self, liveness_mock, p):
        import eventlet

        def liveness_thread():
            while 1:
                if p.ready:
                    return
                eventlet.sleep(self.interval / 10)
                liveness_mock()

        eventlet.spawn(liveness_thread)
        eventlet.sleep(0)


class test_Drainer(DrainerTests):
    @pytest.fixture(autouse=True)
    def setup_drainer(self):
        self.drainer = self.get_drainer('default')

    def result_consumer_drain_events(self, timeout=None):
        time.sleep(timeout)

    def schedule_liveness_thread(self, liveness_mock, p):
        def liveness_thread():
            while 1:
                if p.ready:
                    return
                time.sleep(self.interval / 10)
                liveness_mock()

        threading.Thread(target=liveness_thread).start()


@skip.unless_module('gevent')
class test_GeventDrainer(DrainerTests):
    @pytest.fixture(autouse=True)
    def setup_drainer(self):
        self.drainer = self.get_drainer('gevent')

    def result_consumer_drain_events(self, timeout=None):
        import gevent
        gevent.sleep(0)

    def schedule_liveness_thread(self, liveness_mock, p):
        import gevent

        def liveness_thread():
            while 1:
                if p.ready:
                    return
                gevent.sleep(self.interval / 10)
                liveness_mock()

        gevent.spawn(liveness_thread)
        gevent.sleep(0)
