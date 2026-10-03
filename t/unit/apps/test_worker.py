from unittest.mock import Mock, patch

import pytest

from celery.apps.worker import Worker, _shutdown_handler
from celery.worker import state


class test_Worker_purge:
    """Purging at startup must honour the broker connection retry settings.

    ``purge_messages`` runs from ``on_start``, before the consumer blueprint
    exists, so the retry handling in :mod:`celery.worker.consumer` never sees
    it.  See https://github.com/celery/celery/issues/10102.
    """

    @pytest.fixture(autouse=True)
    def _setup_app(self, app):
        self.app = app

    def _worker(self, **conf):
        self.app.conf.update(conf)
        worker = Worker(app=self.app, hostname='test@example.com')
        return worker

    def test_purge_retries_connection_on_startup(self):
        worker = self._worker(
            broker_connection_retry_on_startup=True,
            broker_connection_max_retries=7,
        )
        connection = Mock(name='connection')

        with patch.object(self.app, 'connection_for_write') as conn_for_write:
            conn_for_write.return_value.__enter__ = Mock(return_value=connection)
            conn_for_write.return_value.__exit__ = Mock(return_value=None)
            with patch.object(self.app.control, 'purge', return_value=0):
                worker.purge_messages()

        connection.ensure_connection.assert_called_once()
        assert connection.ensure_connection.call_args[0][1] == 7
        connection.connect.assert_not_called()

    def test_purge_does_not_retry_when_disabled(self):
        worker = self._worker(
            broker_connection_retry_on_startup=False,
        )
        connection = Mock(name='connection')

        with patch.object(self.app, 'connection_for_write') as conn_for_write:
            conn_for_write.return_value.__enter__ = Mock(return_value=connection)
            conn_for_write.return_value.__exit__ = Mock(return_value=None)
            with patch.object(self.app.control, 'purge', return_value=0):
                worker.purge_messages()

        connection.connect.assert_called_once_with()
        connection.ensure_connection.assert_not_called()

    def test_purge_falls_back_to_broker_connection_retry(self):
        # Apps that never set the newer setting keep the old one's behaviour.
        worker = self._worker(
            broker_connection_retry_on_startup=None,
            broker_connection_retry=True,
        )
        connection = Mock(name='connection')

        with patch.object(self.app, 'connection_for_write') as conn_for_write:
            conn_for_write.return_value.__enter__ = Mock(return_value=connection)
            conn_for_write.return_value.__exit__ = Mock(return_value=None)
            with patch.object(self.app.control, 'purge', return_value=0):
                worker.purge_messages()

        connection.ensure_connection.assert_called_once()
        connection.connect.assert_not_called()


class test_shutdown_handler:
    """The shutdown flag must be set even if the callback/say/signal block raises.

    ``_handle_request`` used to run ``callback`` / ``safe_say`` /
    ``signals.worker_shutting_down.send`` *before* setting
    ``state.should_stop`` / ``state.should_terminate``. If any of those
    raised (e.g. a signal arriving while the process is already mid write
    to the same stream from another thread), the state flag was never
    set and the worker kept consuming new broker messages indefinitely.
    """

    def setup_method(self):
        state.should_stop = None
        state.should_terminate = None
        # Avoid touching real OS signal state: celery.platforms.signals
        # normally calls signal.signal() on assignment, which we don't
        # want running inside a test process. _shutdown_handler only
        # needs somewhere to stash the closure it builds.
        self._signals_patch = patch('celery.apps.worker.platforms.signals', {})
        self.fake_signals = self._signals_patch.start()

    def teardown_method(self):
        self._signals_patch.stop()
        state.should_stop = None
        state.should_terminate = None

    def _install(self, worker, sig='SIGTERM', how='Warm', callback=None, exitcode=0, verbose=True):
        _shutdown_handler(worker, sig=sig, how=how, callback=callback, exitcode=exitcode, verbose=verbose)
        return self.fake_signals[sig]

    @patch('celery.apps.worker.current_process')
    def test_sets_should_stop_even_if_safe_say_raises(self, current_process):
        current_process.return_value._name = 'MainProcess'
        worker = Mock()
        worker.hostname = 'worker1@example.com'

        with patch('celery.apps.worker.safe_say', side_effect=RuntimeError('boom')):
            handler = self._install(worker, sig='SIGTERM', how='Warm', exitcode=0)
            with pytest.raises(RuntimeError):
                handler()

        # Despite safe_say() raising, the shutdown flag must already be set:
        # it's assigned before the callback/logging/signal-dispatch block runs.
        assert state.should_stop == 0

    @patch('celery.apps.worker.current_process')
    def test_sets_should_stop_even_if_callback_raises(self, current_process):
        current_process.return_value._name = 'MainProcess'
        worker = Mock()
        worker.hostname = 'worker1@example.com'
        bad_callback = Mock(side_effect=RuntimeError('boom'))

        handler = self._install(worker, sig='SIGTERM', how='Warm', callback=bad_callback, exitcode=0)
        with pytest.raises(RuntimeError):
            handler()

        assert state.should_stop == 0

    @patch('celery.apps.worker.current_process')
    def test_sets_should_terminate_for_cold_shutdown(self, current_process):
        current_process.return_value._name = 'MainProcess'
        worker = Mock()
        worker.hostname = 'worker1@example.com'

        handler = self._install(worker, sig='SIGQUIT', how='Cold', exitcode=1)
        handler()

        assert state.should_terminate == 1

    @patch('celery.apps.worker.current_process')
    def test_normal_path_still_says_and_sends_signal(self, current_process):
        current_process.return_value._name = 'MainProcess'
        worker = Mock()
        worker.hostname = 'worker1@example.com'

        with patch('celery.apps.worker.safe_say') as safe_say_mock:
            with patch('celery.apps.worker.signals') as signals_mock:
                handler = self._install(worker, sig='SIGTERM', how='Warm', exitcode=0, verbose=True)
                handler()

        assert state.should_stop == 0
        safe_say_mock.assert_called_once()
        signals_mock.worker_shutting_down.send.assert_called_once()

    @patch('celery.apps.worker.current_process')
    def test_non_main_process_skips_say_but_still_sets_flag(self, current_process):
        current_process.return_value._name = 'ForkPoolWorker-1'
        worker = Mock()
        worker.hostname = 'worker1@example.com'

        with patch('celery.apps.worker.safe_say') as safe_say_mock:
            handler = self._install(worker, sig='SIGTERM', how='Warm', exitcode=0)
            handler()

        assert state.should_stop == 0
        safe_say_mock.assert_not_called()


class test_safe_say:

    def test_writes_via_original_os_write(self):
        f = Mock()
        f.fileno.return_value = 7
        with patch('celery.apps.worker._original_os_write') as os_write:
            from celery.apps.worker import safe_say
            safe_say('worker: Warm shutdown (MainProcess)', f)
        os_write.assert_called_once()
        assert os_write.call_args[0][0] == 7

    def test_swallows_oserror_from_os_write(self):
        f = Mock()
        f.fileno.return_value = 7
        from celery.apps.worker import safe_say
        with patch('celery.apps.worker._original_os_write', side_effect=OSError()):
            safe_say('worker: Warm shutdown (MainProcess)', f)  # must not raise
