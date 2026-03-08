import logging
import os
import socket
import sys
import threading
import time
from unittest.mock import Mock, patch

import pytest
from vine import promise

from celery.backends.asynchronous import E_CELERY_RESTART_REQUIRED, BaseResultConsumer, greenletDrainer
from celery.backends.base import Backend
from celery.utils import cached_property

# ---- helpers ---------------------------------------------------------------


def _make_consumer(app, environment='default'):
    """Create a BaseResultConsumer with a mocked drainer environment."""
    with patch('celery.backends.asynchronous.detect_environment') as det:
        det.return_value = environment
        backend = Backend(app)
        consumer = BaseResultConsumer(
            backend, app, backend.accept,
            pending_results={}, pending_messages={},
        )
    return consumer


# ---------------------------------------------------------------------------
# 1. Drainer (default / synchronous) -- no gevent / eventlet needed
# ---------------------------------------------------------------------------

class test_Drainer_without_greenlets:

    # -- drain_events_until: normal flow ------------------------------------

    def test_drain_fulfils_promise(self, app):
        """Loop exits once the promise is fulfilled."""
        consumer = _make_consumer(app)
        drainer = consumer.drainer
        p = promise()
        calls = [0]

        def wait(timeout=None):
            calls[0] += 1
            if calls[0] >= 3:
                p('done')

        for _ in drainer.drain_events_until(
                p, wait=wait, interval=0.01, timeout=5):
            pass

        assert p.ready
        assert calls[0] >= 3

    def test_drain_calls_on_interval(self, app):
        """on_interval callback is invoked every iteration."""
        consumer = _make_consumer(app)
        drainer = consumer.drainer
        p = promise()
        on_interval = Mock()
        calls = [0]

        def wait(timeout=None):
            calls[0] += 1
            if calls[0] >= 3:
                p('done')

        for _ in drainer.drain_events_until(
                p, wait=wait, interval=0.01, timeout=5,
                on_interval=on_interval):
            pass

        assert on_interval.call_count >= 2

    def test_drain_raises_timeout(self, app):
        """socket.timeout raised when total elapsed time exceeds *timeout*."""
        consumer = _make_consumer(app)
        drainer = consumer.drainer
        p = promise()

        def wait(timeout=None):
            time.sleep(0.02)

        with pytest.raises(socket.timeout):
            for _ in drainer.drain_events_until(
                    p, wait=wait, interval=0.01, timeout=0.05):
                pass

        assert not p.ready

    def test_drain_uses_result_consumer_drain_events_by_default(self, app):
        """When *wait* is None, result_consumer.drain_events is used."""
        consumer = _make_consumer(app)
        drainer = consumer.drainer
        p = promise()
        calls = [0]

        def mock_drain(timeout=None):
            calls[0] += 1
            if calls[0] >= 2:
                p('done')

        consumer.drain_events = mock_drain

        for _ in drainer.drain_events_until(p, interval=0.01, timeout=5):
            pass

        assert p.ready
        assert calls[0] >= 2

    # -- drain_events_until: socket.timeout from wait -----------------------

    def test_drain_swallows_socket_timeout_from_wait(self, app):
        """socket.timeout raised inside wait() must be silently caught."""
        consumer = _make_consumer(app)
        drainer = consumer.drainer
        p = promise()
        calls = [0]

        def wait(timeout=None):
            calls[0] += 1
            if calls[0] <= 2:
                raise socket.timeout('idle')
            p('done')

        for _ in drainer.drain_events_until(
                p, wait=wait, interval=0.01, timeout=5):
            pass

        assert p.ready

    # -- drain_events_until: OSError from wait ------------------------------

    def test_drain_catches_oserror_and_logs(self, app):
        """OSError from wait() must be caught, logged, loop continues."""
        consumer = _make_consumer(app)
        drainer = consumer.drainer
        p = promise()
        calls = [0]

        def wait(timeout=None):
            calls[0] += 1
            if calls[0] <= 2:
                raise OSError('broker away')
            p('done')

        with patch.object(logging, 'warning') as mock_warn:
            for _ in drainer.drain_events_until(
                    p, wait=wait, interval=0.01, timeout=5):
                pass

        assert p.ready
        assert mock_warn.call_count >= 2

    # -- wait_for -----------------------------------------------------------

    def test_wait_for_calls_wait_with_timeout(self, app):
        """Drainer.wait_for delegates to the wait callback."""
        consumer = _make_consumer(app)
        drainer = consumer.drainer
        p = promise()
        wait = Mock()
        drainer.wait_for(p, wait, timeout=0.5)
        wait.assert_called_once_with(timeout=0.5)


# ---------------------------------------------------------------------------
# 2. greenletDrainer -- tested synchronously (no real greenlet spawning)
# ---------------------------------------------------------------------------

class test_greenletDrainer:

    def _make_greenlet_drainer(self, app):
        consumer = _make_consumer(app)
        drainer = greenletDrainer(consumer)
        return drainer

    # -- run: normal stop ---------------------------------------------------

    def test_run_exits_when_stopped(self, app):
        """run() exits cleanly when _stopped is set."""
        drainer = self._make_greenlet_drainer(app)
        calls = [0]

        def drain(timeout=None):
            calls[0] += 1
            if calls[0] >= 3:
                drainer._stopped.set()

        drainer.result_consumer.drain_events = Mock(side_effect=drain)
        drainer.run()

        assert drainer._shutdown.is_set()
        assert drainer._exc is None

    # -- run: socket.timeout is swallowed -----------------------------------

    def test_run_swallows_socket_timeout(self, app):
        """socket.timeout inside run() must be silently caught."""
        drainer = self._make_greenlet_drainer(app)
        calls = [0]

        def drain(timeout=None):
            calls[0] += 1
            if calls[0] <= 3:
                raise socket.timeout('idle')
            drainer._stopped.set()

        drainer.result_consumer.drain_events = Mock(side_effect=drain)
        drainer.run()

        assert calls[0] >= 4
        assert drainer._exc is None

    # -- run: OSError is caught and logged ----------------------------------

    def test_run_catches_oserror_and_logs(self, app):
        """OSError in run() must be caught/logged, loop continues."""
        drainer = self._make_greenlet_drainer(app)
        calls = [0]

        def drain(timeout=None):
            calls[0] += 1
            if calls[0] <= 3:
                raise OSError('connection reset')
            drainer._stopped.set()

        drainer.result_consumer.drain_events = Mock(side_effect=drain)

        with patch.object(logging, 'warning') as mock_warn, \
                patch('celery.backends.asynchronous.time.sleep') as mock_sleep:
            drainer.run()

        assert calls[0] >= 4
        assert mock_warn.call_count >= 3
        # backoff sleep should have been called once per OSError
        assert mock_sleep.call_count >= 3
        assert drainer._exc is None

    # -- run: unexpected Exception is stored and re-raised ------------------

    def test_run_stores_and_reraises_unexpected_exception(self, app):
        """Non-OSError / non-timeout exceptions must propagate and be stored."""
        drainer = self._make_greenlet_drainer(app)

        def drain(timeout=None):
            raise RuntimeError('unexpected')

        drainer.result_consumer.drain_events = Mock(side_effect=drain)

        with pytest.raises(RuntimeError, match='unexpected'):
            drainer.run()

        assert drainer._exc is not None
        assert drainer._shutdown.is_set()

    # -- _ensure_not_shut_down ----------------------------------------------

    def test_ensure_not_shut_down_raises_stored_exc(self, app):
        """If run() failed, _ensure_not_shut_down re-raises the exception."""
        drainer = self._make_greenlet_drainer(app)
        drainer._shutdown.set()
        drainer._exc = ValueError('boom')

        with pytest.raises(ValueError, match='boom'):
            drainer._ensure_not_shut_down()

    def test_ensure_not_shut_down_raises_restart_msg(self, app):
        """If stopped cleanly, _ensure_not_shut_down raises restart msg."""
        drainer = self._make_greenlet_drainer(app)
        drainer._shutdown.set()
        drainer._exc = None

        with pytest.raises(Exception, match=E_CELERY_RESTART_REQUIRED):
            drainer._ensure_not_shut_down()

    def test_ensure_not_shut_down_noop_when_running(self, app):
        """No-op when _shutdown is not set."""
        drainer = self._make_greenlet_drainer(app)
        # Should not raise
        drainer._ensure_not_shut_down()

    # -- start / stop -------------------------------------------------------

    def test_start_spawns_and_waits(self, app):
        """start() calls spawn(run) and waits for _started."""
        drainer = self._make_greenlet_drainer(app)

        def fake_spawn(func):
            # Run synchronously with immediate stop.
            drainer._stopped.set()
            func()

        drainer.spawn = fake_spawn
        drainer.result_consumer.drain_events = Mock(
            side_effect=lambda timeout=None: drainer._stopped.set()
        )
        drainer.start()

        assert drainer._started.is_set()
        assert drainer._shutdown.is_set()

    def test_start_raises_if_already_shut_down(self, app):
        """start() raises if drainer already completed."""
        drainer = self._make_greenlet_drainer(app)
        drainer._shutdown.set()

        with pytest.raises(Exception, match=E_CELERY_RESTART_REQUIRED):
            drainer.start()

    def test_stop_signals_and_waits(self, app):
        """stop() sets _stopped and waits for _shutdown."""
        drainer = self._make_greenlet_drainer(app)
        # Pre-set _shutdown so wait returns immediately.
        drainer._shutdown.set()
        drainer.stop()

        assert drainer._stopped.is_set()


# ---------------------------------------------------------------------------
# 3. Integration tests with real greenlet runtimes (gevent + eventlet)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def setup_eventlet():
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

    def test_drain_catches_and_logs_oserror(self):
        p = promise()

        def fulfill():
            self.sleep(self.interval * 2)
            p('done')

        t = self.schedule_thread(fulfill)

        state = {'n': 0}

        def flaky(*args, **kwargs):
            state['n'] += 1
            if state['n'] == 1:
                raise OSError('simulated broker restart')
            # Yield to hub so the promise thread can run.
            self.result_consumer_drain_events(
                timeout=kwargs.get('timeout', None),
            )

        with patch.object(
            self.drainer.result_consumer, 'drain_events',
            side_effect=flaky,
        ):
            with patch('logging.warning') as mock_warn:
                for _ in self.drainer.drain_events_until(
                        p, interval=self.interval,
                        timeout=self.MAX_TIMEOUT):
                    pass

        self.teardown_thread(t)
        assert p.ready
        assert mock_warn.called


class GreenletDrainerTests(DrainerTests):
    def test_drain_raises_when_greenlet_already_exited(self):
        with patch.object(self.drainer.result_consumer, 'drain_events', side_effect=Exception("Test Exception")):
            thread = self.schedule_thread(self.drainer.run)

            with pytest.raises(Exception, match="Test Exception"):
                p = promise()

                for _ in self.drainer.drain_events_until(p, interval=self.interval):
                    pass

            self.teardown_thread(thread)

    def test_drain_raises_while_waiting_on_exiting_greenlet(self):
        with patch.object(self.drainer.result_consumer, 'drain_events', side_effect=Exception("Test Exception")):
            with pytest.raises(Exception, match="Test Exception"):
                p = promise()

                for _ in self.drainer.drain_events_until(p, interval=self.interval):
                    pass

    def test_start_raises_if_previous_error_in_run(self):
        with patch.object(self.drainer.result_consumer, 'drain_events', side_effect=Exception("Test Exception")):
            thread = self.schedule_thread(self.drainer.run)

            with pytest.raises(Exception, match="Test Exception"):
                self.drainer.start()

            self.teardown_thread(thread)

    def test_start_raises_if_drainer_already_stopped(self):
        with patch.object(self.drainer.result_consumer, 'drain_events', side_effect=lambda **_: self.sleep(0)):
            thread = self.schedule_thread(self.drainer.run)
            self.drainer.stop()

            with pytest.raises(Exception, match=E_CELERY_RESTART_REQUIRED):
                self.drainer.start()

            self.teardown_thread(thread)

    def test_run_catches_and_logs_oserror(self):
        def flaky(*args, **kwargs):
            if not hasattr(flaky, '_raised'):
                flaky._raised = True
                raise OSError('simulated broker restart in greenlet')
            self.drainer._stopped.set()

        with patch.object(
            self.drainer.result_consumer, 'drain_events',
            side_effect=flaky,
        ):
            with patch('logging.warning') as mock_warn:
                t = self.schedule_thread(self.drainer.run)
                self.teardown_thread(t)

        assert mock_warn.called
        assert 'connection error during drain_events' in mock_warn.call_args[0][0]
        assert self.drainer._exc is None


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="hangs forever intermittently on windows"
)
class test_EventletDrainer(GreenletDrainerTests):
    @pytest.fixture(autouse=True)
    def setup_drainer(self):
        pytest.importorskip('eventlet')
        self.drainer = self.get_drainer('eventlet')

    @cached_property
    def sleep(self):
        from eventlet import sleep
        return sleep

    def result_consumer_drain_events(self, timeout=None):
        import eventlet

        # `drain_events` of asynchronous backends with pubsub have to sleep
        # while waiting events for not more then `interval` timeout,
        # but events may coming sooner
        eventlet.sleep(timeout/10)

    def schedule_thread(self, thread):
        import eventlet
        g = eventlet.spawn(thread)
        eventlet.sleep(0)
        return g

    def teardown_thread(self, thread):
        try:
            # eventlet's wait() propagates any errors on the green thread, unlike
            # similar methods in gevent or python's threading library
            thread.wait()
        except Exception:
            pass


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


class test_GeventDrainer(GreenletDrainerTests):
    @pytest.fixture(autouse=True)
    def setup_drainer(self):
        pytest.importorskip('gevent')
        self.drainer = self.get_drainer('gevent')

    @cached_property
    def sleep(self):
        from gevent import sleep
        return sleep

    def result_consumer_drain_events(self, timeout=None):
        import gevent

        # `drain_events` of asynchronous backends with pubsub have to sleep
        # while waiting events for not more then `interval` timeout,
        # but events may coming sooner
        gevent.sleep(timeout/10)

    def schedule_thread(self, thread):
        import gevent
        g = gevent.spawn(thread)
        gevent.sleep(0)
        return g

    def teardown_thread(self, thread):
        import gevent
        gevent.wait([thread])
