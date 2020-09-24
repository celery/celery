import os
import socket
import threading
import time
from unittest.mock import Mock, patch

import pytest
from vine import promise

from celery.backends.asynchronous import BaseResultConsumer
from celery.backends.base import Backend
from celery.utils import cached_property

pytest.importorskip('gevent')


@pytest.fixture(autouse=True)
def setup_eventlet():
    # By default eventlet will patch the DNS resolver when imported.
    os.environ.update(EVENTLET_NO_GREENDNS='yes')


class DrainerTests:
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

    @cached_property
    def sleep(self):
        """
        Sleep on the event loop.
        """
        raise NotImplementedError

    def schedule_thread(self, thread):
        """
        Set up a thread that runs on the event loop.
        """
        raise NotImplementedError

    def teardown_thread(self, thread):
        """
        Wait for a thread to stop.
        """
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
            self.sleep(self.interval * 2)
            p('done')

        fulfill_thread = self.schedule_thread(fulfill_promise_thread)

        on_interval = Mock()
        for _ in self.drainer.drain_events_until(p,
                                                 on_interval=on_interval,
                                                 interval=self.interval,
                                                 timeout=self.MAX_TIMEOUT):
            pass

        self.teardown_thread(fulfill_thread)

        assert p.ready, 'Should have terminated with promise being ready'
        assert on_interval.call_count < 20, 'Should have limited number of calls to on_interval'

    def test_drain_does_not_block_event_loop(self):
        """
        This test makes sure that other greenlets can still operate while drain_events_until is
        running.
        """
        p = promise()
        liveness_mock = Mock()

        def fulfill_promise_thread():
            self.sleep(self.interval * 2)
            p('done')

        def liveness_thread():
            while 1:
                if p.ready:
                    return
                self.sleep(self.interval / 10)
                liveness_mock()

        fulfill_thread = self.schedule_thread(fulfill_promise_thread)
        liveness_thread = self.schedule_thread(liveness_thread)

        on_interval = Mock()
        for _ in self.drainer.drain_events_until(p,
                                                 on_interval=on_interval,
                                                 interval=self.interval,
                                                 timeout=self.MAX_TIMEOUT):
            pass

        self.teardown_thread(fulfill_thread)
        self.teardown_thread(liveness_thread)

        assert p.ready, 'Should have terminated with promise being ready'
        assert on_interval.call_count <= liveness_mock.call_count, \
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


class test_EventletDrainer(DrainerTests):
    @pytest.fixture(autouse=True)
    def setup_drainer(self):
        self.drainer = self.get_drainer('eventlet')

    @cached_property
    def sleep(self):
        from eventlet import sleep
        return sleep

    def result_consumer_drain_events(self, timeout=None):
        import eventlet
        eventlet.sleep(0)

    def schedule_thread(self, thread):
        import eventlet
        g = eventlet.spawn(thread)
        eventlet.sleep(0)
        return g

    def teardown_thread(self, thread):
        thread.wait()


class test_Drainer(DrainerTests):
    @pytest.fixture(autouse=True)
    def setup_drainer(self):
        self.drainer = self.get_drainer('default')

    @cached_property
    def sleep(self):
        from time import sleep
        return sleep

    def result_consumer_drain_events(self, timeout=None):
        time.sleep(timeout)

    def schedule_thread(self, thread):
        t = threading.Thread(target=thread)
        t.start()
        return t

    def teardown_thread(self, thread):
        thread.join()


class test_GeventDrainer(DrainerTests):
    @pytest.fixture(autouse=True)
    def setup_drainer(self):
        self.drainer = self.get_drainer('gevent')

    @cached_property
    def sleep(self):
        from gevent import sleep
        return sleep

    def result_consumer_drain_events(self, timeout=None):
        import gevent
        gevent.sleep(0)

    def schedule_thread(self, thread):
        import gevent
        g = gevent.spawn(thread)
        gevent.sleep(0)
        return g

    def teardown_thread(self, thread):
        import gevent
        gevent.wait([thread])
