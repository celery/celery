from unittest.mock import Mock, patch

import pytest

from celery.apps.worker import Worker, _shutdown_handler, safe_say
from celery.worker import WorkController, state


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


class test_Worker_setup_defaults:
    """``setup_defaults`` must only run once per worker.

    ``WorkController.__init__`` already calls it, so ``on_after_init`` calling
    it again bypassed subclass overrides.
    See https://github.com/celery/celery/issues/7278.
    """

    @pytest.fixture(autouse=True)
    def _setup_app(self, app):
        self.app = app

    def test_setup_defaults_called_once(self):
        with patch.object(WorkController, 'setup_defaults',
                          autospec=True,
                          side_effect=WorkController.setup_defaults) as setup_defaults:
            Worker(app=self.app, hostname='test@example.com',
                   concurrency=2, pool='solo')
        setup_defaults.assert_called_once()

    def test_setup_defaults_override_is_kept(self):
        class CustomWorker(Worker):

            def setup_defaults(self, **kwargs):
                super().setup_defaults(**kwargs)
                self.concurrency = 7

        worker = CustomWorker(app=self.app, hostname='test@example.com',
                              concurrency=2, pool='solo')
        assert worker.concurrency == 7


class test_safe_say:
    """safe_say() must never raise, even if the underlying write fails.

    It's a best-effort human-readable banner: a failure to print it must
    never be able to abort a caller's control flow (notably
    _shutdown_handler, see test_shutdown_handler below).
    """

    def test_writes_via_original_os_write(self):
        with patch('celery.apps.worker._original_os_write') as os_write:
            safe_say('worker: Warm shutdown (MainProcess)', f=Mock(fileno=Mock(return_value=2)))
        os_write.assert_called_once()
        assert b'worker: Warm shutdown (MainProcess)' in os_write.call_args[0][1]

    def test_swallows_oserror(self):
        with patch('celery.apps.worker._original_os_write', side_effect=OSError('EPIPE')):
            # must not raise
            safe_say('worker: Warm shutdown (MainProcess)', f=Mock(fileno=Mock(return_value=2)))


class test_shutdown_handler:
    """_shutdown_handler's signal handler must set the state flag
    unconditionally, even if the callback/say/signal-dispatch block raises,
    *without* making the flag visible before that block runs on the normal
    (non-raising) path -- on_cold_shutdown() relies on should_terminate
    staying unset while it waits out worker_soft_shutdown_timeout, and
    state.maybe_shutdown()/Request.on_timeout()/on_failure() all branch on
    should_terminate during that window.
    """

    def setup_method(self):
        state.should_stop = None
        state.should_terminate = None
        # Avoid touching real OS signal state: celery.platforms.signals
        # normally calls signal.signal() on assignment, which we don't want
        # running inside a test process. _shutdown_handler only needs
        # somewhere to stash the closure it builds.
        self._signals_patch = patch('celery.apps.worker.platforms.signals', {})
        self.fake_signals = self._signals_patch.start()

    def teardown_method(self):
        self._signals_patch.stop()
        state.should_stop = None
        state.should_terminate = None

    def _install(self, worker, sig='SIGTERM', how='Warm', callback=None, exitcode=0):
        _shutdown_handler(worker, sig=sig, how=how, callback=callback, exitcode=exitcode)
        return self.fake_signals[sig]

    @patch('celery.apps.worker.current_process')
    def test_sets_should_stop_even_if_safe_say_raises(self, current_process):
        current_process.return_value._name = 'MainProcess'
        worker = Mock()
        handler = self._install(worker, how='Warm')

        with patch('celery.apps.worker.safe_say', side_effect=RuntimeError('reentrant call')):
            with pytest.raises(RuntimeError, match='reentrant call'):
                handler()

        assert state.should_stop == 0

    @patch('celery.apps.worker.current_process')
    def test_sets_should_terminate_even_if_callback_raises(self, current_process):
        current_process.return_value._name = 'MainProcess'
        worker = Mock()
        callback = Mock(side_effect=RuntimeError('boom'))
        handler = self._install(worker, how='Cold', callback=callback)

        with pytest.raises(RuntimeError, match='boom'):
            handler()

        # the flag must still be set even though the callback raised and
        # the exception propagates (it is not swallowed)
        assert state.should_terminate == 0

    @patch('celery.apps.worker.current_process')
    def test_cold_shutdown_flag_not_set_during_callback(self, current_process):
        """Pins the ordering: should_terminate must NOT be visible yet while
        the callback (on_cold_shutdown in production) is still running, since
        it may be mid soft-shutdown-wait and branches on this flag itself.

        This is the regression this test guards against: an earlier version
        of this fix set the flag *before* invoking the callback, which would
        have made maybe_shutdown()/Request.on_timeout()/on_failure() see
        should_terminate as already set during the soft-shutdown window.
        """
        current_process.return_value._name = 'MainProcess'
        worker = Mock()
        seen = {}

        def callback(w):
            seen['should_terminate_during_callback'] = state.should_terminate

        handler = self._install(worker, how='Cold', callback=callback)
        handler()

        assert seen['should_terminate_during_callback'] is None
        assert state.should_terminate == 0

    @patch('celery.apps.worker.current_process')
    def test_normal_path_still_calls_say_and_signal(self, current_process):
        current_process.return_value._name = 'MainProcess'
        worker = Mock(hostname='worker@host')
        handler = self._install(worker, how='Warm')

        with patch('celery.apps.worker.safe_say') as safe_say_mock, \
                patch('celery.apps.worker.signals.worker_shutting_down.send') as send_mock:
            handler()

        safe_say_mock.assert_called_once()
        send_mock.assert_called_once()
        assert state.should_stop == 0

    @patch('celery.apps.worker.current_process')
    def test_non_mainprocess_skips_say_but_still_sets_state(self, current_process):
        current_process.return_value._name = 'ForkPoolWorker-1'
        worker = Mock()
        callback = Mock()
        handler = self._install(worker, how='Warm', callback=callback)

        with patch('celery.apps.worker.safe_say') as safe_say_mock:
            handler()

        callback.assert_not_called()
        safe_say_mock.assert_not_called()
        assert state.should_stop == 0


class test_autoreload:

    def test_start_autoreloader(self):
        from celery.apps import worker as worker_module
        w = Mock(autoreload=True)
        w.app.IS_macOS = False
        with patch('celery.worker.autoreload.Autoreloader') as Reloader, \
                patch.object(worker_module, 'restart_worker') as restart:
            worker_module.Worker.start_autoreloader(w)
            on_change = Reloader.call_args[0][0]
            Reloader.return_value.start.assert_called_once_with()
            on_change('/path/mod.py')
            restart.assert_called_once_with()

    def test_start_autoreloader_not_supported_on_macos(self):
        from celery.apps import worker as worker_module
        w = Mock(autoreload=True)
        w.app.IS_macOS = True
        with patch('celery.worker.autoreload.Autoreloader') as Reloader:
            worker_module.Worker.start_autoreloader(w)
        Reloader.assert_not_called()
