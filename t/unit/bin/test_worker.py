from __future__ import absolute_import, unicode_literals

import logging
import os
import sys

import pytest
from billiard.process import current_process
from case import Mock, mock, patch, skip
from kombu import Exchange, Queue

from celery import platforms, signals
from celery.app import trace
from celery.apps import worker as cd
from celery.bin.worker import main as worker_main
from celery.bin.worker import worker
from celery.exceptions import (ImproperlyConfigured, WorkerShutdown,
                               WorkerTerminate)
from celery.platforms import EX_FAILURE, EX_OK
from celery.worker import state


@pytest.fixture(autouse=True)
def reset_worker_optimizations():
    yield
    trace.reset_worker_optimizations()


class Worker(cd.Worker):
    redirect_stdouts = False

    def start(self, *args, **kwargs):
        self.on_start()


class test_Worker:
    Worker = Worker

    def test_queues_string(self):
        with mock.stdouts():
            w = self.app.Worker()
            w.setup_queues('foo,bar,baz')
            assert 'foo' in self.app.amqp.queues

    def test_cpu_count(self):
        with mock.stdouts():
            with patch('celery.worker.worker.cpu_count') as cpu_count:
                cpu_count.side_effect = NotImplementedError()
                w = self.app.Worker(concurrency=None)
                assert w.concurrency == 2
            w = self.app.Worker(concurrency=5)
            assert w.concurrency == 5

    def test_windows_B_option(self):
        with mock.stdouts():
            self.app.IS_WINDOWS = True
            with pytest.raises(SystemExit):
                worker(app=self.app).run(beat=True)

    def test_setup_concurrency_very_early(self):
        x = worker()
        x.run = Mock()
        with pytest.raises(ImportError):
            x.execute_from_commandline(['worker', '-P', 'xyzybox'])

    def test_run_from_argv_basic(self):
        x = worker(app=self.app)
        x.run = Mock()
        x.maybe_detach = Mock()

        def run(*args, **kwargs):
            pass
        x.run = run
        x.run_from_argv('celery', [])
        x.maybe_detach.assert_called()

    def test_maybe_detach(self):
        x = worker(app=self.app)
        with patch('celery.bin.worker.detached_celeryd') as detached:
            x.maybe_detach([])
            detached.assert_not_called()
            with pytest.raises(SystemExit):
                x.maybe_detach(['--detach'])
            detached.assert_called()

    def test_invalid_loglevel_gives_error(self):
        with mock.stdouts():
            x = worker(app=self.app)
            with pytest.raises(SystemExit):
                x.run(loglevel='GRIM_REAPER')

    def test_no_loglevel(self):
        self.app.Worker = Mock()
        worker(app=self.app).run(loglevel=None)

    def test_tasklist(self):
        worker = self.app.Worker()
        assert worker.app.tasks
        assert worker.app.finalized
        assert worker.tasklist(include_builtins=True)
        worker.tasklist(include_builtins=False)

    def test_extra_info(self):
        worker = self.app.Worker()
        worker.loglevel = logging.WARNING
        assert not worker.extra_info()
        worker.loglevel = logging.INFO
        assert worker.extra_info()

    def test_loglevel_string(self):
        with mock.stdouts():
            worker = self.Worker(app=self.app, loglevel='INFO')
            assert worker.loglevel == logging.INFO

    def test_run_worker(self, patching):
        handlers = {}

        class Signals(platforms.Signals):

            def __setitem__(self, sig, handler):
                handlers[sig] = handler

        patching.setattr('celery.platforms.signals', Signals())
        with mock.stdouts():
            w = self.Worker(app=self.app)
            w._isatty = False
            w.on_start()
            for sig in 'SIGINT', 'SIGHUP', 'SIGTERM':
                assert sig in handlers

            handlers.clear()
            w = self.Worker(app=self.app)
            w._isatty = True
            w.on_start()
            for sig in 'SIGINT', 'SIGTERM':
                assert sig in handlers
            assert 'SIGHUP' not in handlers

    def test_startup_info(self):
        with mock.stdouts():
            worker = self.Worker(app=self.app)
            worker.on_start()
            assert worker.startup_info()
            worker.loglevel = logging.DEBUG
            assert worker.startup_info()
            worker.loglevel = logging.INFO
            assert worker.startup_info()
            worker.autoscale = 13, 10
            assert worker.startup_info()

            prev_loader = self.app.loader
            worker = self.Worker(
                app=self.app,
                queues='foo,bar,baz,xuzzy,do,re,mi',
            )
            with patch('celery.apps.worker.qualname') as qualname:
                qualname.return_value = 'acme.backed_beans.Loader'
                assert worker.startup_info()

            with patch('celery.apps.worker.qualname') as qualname:
                qualname.return_value = 'celery.loaders.Loader'
                assert worker.startup_info()

            from celery.loaders.app import AppLoader
            self.app.loader = AppLoader(app=self.app)
            assert worker.startup_info()

            self.app.loader = prev_loader
            worker.task_events = True
            assert worker.startup_info()

            # test when there are too few output lines
            # to draft the ascii art onto
            prev, cd.ARTLINES = cd.ARTLINES, ['the quick brown fox']
            try:
                assert worker.startup_info()
            finally:
                cd.ARTLINES = prev

    def test_run(self):
        with mock.stdouts():
            self.Worker(app=self.app).on_start()
            self.Worker(app=self.app, purge=True).on_start()
            worker = self.Worker(app=self.app)
            worker.on_start()

    def test_purge_messages(self):
        with mock.stdouts():
            self.Worker(app=self.app).purge_messages()

    def test_init_queues(self):
        with mock.stdouts():
            app = self.app
            c = app.conf
            app.amqp.queues = app.amqp.Queues({
                'celery': {
                    'exchange': 'celery',
                    'routing_key': 'celery',
                },
                'video': {
                    'exchange': 'video',
                    'routing_key': 'video',
                },
            })
            worker = self.Worker(app=self.app)
            worker.setup_queues(['video'])
            assert 'video' in app.amqp.queues
            assert 'video' in app.amqp.queues.consume_from
            assert 'celery' in app.amqp.queues
            assert 'celery' not in app.amqp.queues.consume_from

            c.task_create_missing_queues = False
            del(app.amqp.queues)
            with pytest.raises(ImproperlyConfigured):
                self.Worker(app=self.app).setup_queues(['image'])
            del(app.amqp.queues)
            c.task_create_missing_queues = True
            worker = self.Worker(app=self.app)
            worker.setup_queues(['image'])
            assert 'image' in app.amqp.queues.consume_from
            assert app.amqp.queues['image'] == Queue(
                'image', Exchange('image'),
                routing_key='image',
            )

    def test_autoscale_argument(self):
        with mock.stdouts():
            worker1 = self.Worker(app=self.app, autoscale='10,3')
            assert worker1.autoscale == [10, 3]
            worker2 = self.Worker(app=self.app, autoscale='10')
            assert worker2.autoscale == [10, 0]

    def test_include_argument(self):
        worker1 = self.Worker(app=self.app, include='os')
        assert worker1.include == ['os']
        worker2 = self.Worker(app=self.app,
                              include='os,sys')
        assert worker2.include == ['os', 'sys']
        self.Worker(app=self.app, include=['os', 'sys'])

    def test_unknown_loglevel(self):
        with mock.stdouts():
            with pytest.raises(SystemExit):
                worker(app=self.app).run(loglevel='ALIEN')
            worker1 = self.Worker(app=self.app, loglevel=0xFFFF)
            assert worker1.loglevel == 0xFFFF

    @patch('os._exit')
    @skip.if_win32()
    def test_warns_if_running_as_privileged_user(self, _exit, patching):
        getuid = patching('os.getuid')

        with mock.stdouts() as (_, stderr):
            getuid.return_value = 0
            self.app.conf.accept_content = ['pickle']
            worker = self.Worker(app=self.app)
            worker.on_start()
            _exit.assert_called_with(1)
            patching.setattr('celery.platforms.C_FORCE_ROOT', True)
            worker = self.Worker(app=self.app)
            worker.on_start()
            assert 'a very bad idea' in stderr.getvalue()
            patching.setattr('celery.platforms.C_FORCE_ROOT', False)
            self.app.conf.accept_content = ['json']
            worker = self.Worker(app=self.app)
            worker.on_start()
            assert 'superuser' in stderr.getvalue()

    def test_redirect_stdouts(self):
        with mock.stdouts():
            self.Worker(app=self.app, redirect_stdouts=False)
            with pytest.raises(AttributeError):
                sys.stdout.logger

    def test_on_start_custom_logging(self):
        with mock.stdouts():
            self.app.log.redirect_stdouts = Mock()
            worker = self.Worker(app=self.app, redirect_stoutds=True)
            worker._custom_logging = True
            worker.on_start()
            self.app.log.redirect_stdouts.assert_not_called()

    def test_setup_logging_no_color(self):
        worker = self.Worker(
            app=self.app, redirect_stdouts=False, no_color=True,
        )
        prev, self.app.log.setup = self.app.log.setup, Mock()
        try:
            worker.setup_logging()
            assert not self.app.log.setup.call_args[1]['colorize']
        finally:
            self.app.log.setup = prev

    def test_startup_info_pool_is_str(self):
        with mock.stdouts():
            worker = self.Worker(app=self.app, redirect_stdouts=False)
            worker.pool_cls = 'foo'
            worker.startup_info()

    def test_redirect_stdouts_already_handled(self):
        logging_setup = [False]

        @signals.setup_logging.connect
        def on_logging_setup(**kwargs):
            logging_setup[0] = True

        try:
            worker = self.Worker(app=self.app, redirect_stdouts=False)
            worker.app.log.already_setup = False
            worker.setup_logging()
            assert logging_setup[0]
            with pytest.raises(AttributeError):
                sys.stdout.logger
        finally:
            signals.setup_logging.disconnect(on_logging_setup)

    def test_platform_tweaks_macOS(self):

        class macOSWorker(Worker):
            proxy_workaround_installed = False

            def macOS_proxy_detection_workaround(self):
                self.proxy_workaround_installed = True

        with mock.stdouts():
            worker = macOSWorker(app=self.app, redirect_stdouts=False)

            def install_HUP_nosupport(controller):
                controller.hup_not_supported_installed = True

            class Controller(object):
                pass

            prev = cd.install_HUP_not_supported_handler
            cd.install_HUP_not_supported_handler = install_HUP_nosupport
            try:
                worker.app.IS_macOS = True
                controller = Controller()
                worker.install_platform_tweaks(controller)
                assert controller.hup_not_supported_installed
                assert worker.proxy_workaround_installed
            finally:
                cd.install_HUP_not_supported_handler = prev

    def test_general_platform_tweaks(self):

        restart_worker_handler_installed = [False]

        def install_worker_restart_handler(worker):
            restart_worker_handler_installed[0] = True

        class Controller(object):
            pass

        with mock.stdouts():
            prev = cd.install_worker_restart_handler
            cd.install_worker_restart_handler = install_worker_restart_handler
            try:
                worker = self.Worker(app=self.app)
                worker.app.IS_macOS = False
                worker.install_platform_tweaks(Controller())
                assert restart_worker_handler_installed[0]
            finally:
                cd.install_worker_restart_handler = prev

    def test_on_consumer_ready(self):
        worker_ready_sent = [False]

        @signals.worker_ready.connect
        def on_worker_ready(**kwargs):
            worker_ready_sent[0] = True

        with mock.stdouts():
            self.Worker(app=self.app).on_consumer_ready(object())
            assert worker_ready_sent[0]


@mock.stdouts
class test_funs:

    def test_active_thread_count(self):
        assert cd.active_thread_count()

    @skip.unless_module('setproctitle')
    def test_set_process_status(self):
        worker = Worker(app=self.app, hostname='xyzza')
        prev1, sys.argv = sys.argv, ['Arg0']
        try:
            st = worker.set_process_status('Running')
            assert 'celeryd' in st
            assert 'xyzza' in st
            assert 'Running' in st
            prev2, sys.argv = sys.argv, ['Arg0', 'Arg1']
            try:
                st = worker.set_process_status('Running')
                assert 'celeryd' in st
                assert 'xyzza' in st
                assert 'Running' in st
                assert 'Arg1' in st
            finally:
                sys.argv = prev2
        finally:
            sys.argv = prev1

    def test_parse_options(self):
        cmd = worker()
        cmd.app = self.app
        opts, args = cmd.parse_options('worker', ['--concurrency=512',
                                                  '--heartbeat-interval=10'])
        assert opts['concurrency'] == 512
        assert opts['heartbeat_interval'] == 10

    def test_main(self):
        p, cd.Worker = cd.Worker, Worker
        s, sys.argv = sys.argv, ['worker', '--discard']
        try:
            worker_main(app=self.app)
        finally:
            cd.Worker = p
            sys.argv = s


@mock.stdouts
class test_signal_handlers:

    class _Worker(object):
        hostname = 'foo'
        stopped = False
        terminated = False

        def stop(self, in_sighandler=False):
            self.stopped = True

        def terminate(self, in_sighandler=False):
            self.terminated = True

    def psig(self, fun, *args, **kwargs):
        handlers = {}

        class Signals(platforms.Signals):
            def __setitem__(self, sig, handler):
                handlers[sig] = handler

        p, platforms.signals = platforms.signals, Signals()
        try:
            fun(*args, **kwargs)
            return handlers
        finally:
            platforms.signals = p

    def test_worker_int_handler(self):
        worker = self._Worker()
        handlers = self.psig(cd.install_worker_int_handler, worker)
        next_handlers = {}
        state.should_stop = None
        state.should_terminate = None

        class Signals(platforms.Signals):

            def __setitem__(self, sig, handler):
                next_handlers[sig] = handler

        with patch('celery.apps.worker.active_thread_count') as c:
            c.return_value = 3
            p, platforms.signals = platforms.signals, Signals()
            try:
                handlers['SIGINT']('SIGINT', object())
                assert state.should_stop
                assert state.should_stop == EX_FAILURE
            finally:
                platforms.signals = p
                state.should_stop = None

            try:
                next_handlers['SIGINT']('SIGINT', object())
                assert state.should_terminate
                assert state.should_terminate == EX_FAILURE
            finally:
                state.should_terminate = None

        with patch('celery.apps.worker.active_thread_count') as c:
            c.return_value = 1
            p, platforms.signals = platforms.signals, Signals()
            try:
                with pytest.raises(WorkerShutdown):
                    handlers['SIGINT']('SIGINT', object())
            finally:
                platforms.signals = p

            with pytest.raises(WorkerTerminate):
                next_handlers['SIGINT']('SIGINT', object())

    @skip.unless_module('multiprocessing')
    def test_worker_int_handler_only_stop_MainProcess(self):
        process = current_process()
        name, process.name = process.name, 'OtherProcess'
        with patch('celery.apps.worker.active_thread_count') as c:
            c.return_value = 3
            try:
                worker = self._Worker()
                handlers = self.psig(cd.install_worker_int_handler, worker)
                handlers['SIGINT']('SIGINT', object())
                assert state.should_stop
            finally:
                process.name = name
                state.should_stop = None

        with patch('celery.apps.worker.active_thread_count') as c:
            c.return_value = 1
            try:
                worker = self._Worker()
                handlers = self.psig(cd.install_worker_int_handler, worker)
                with pytest.raises(WorkerShutdown):
                    handlers['SIGINT']('SIGINT', object())
            finally:
                process.name = name
                state.should_stop = None

    def test_install_HUP_not_supported_handler(self):
        worker = self._Worker()
        handlers = self.psig(cd.install_HUP_not_supported_handler, worker)
        handlers['SIGHUP']('SIGHUP', object())

    @skip.unless_module('multiprocessing')
    def test_worker_term_hard_handler_only_stop_MainProcess(self):
        process = current_process()
        name, process.name = process.name, 'OtherProcess'
        try:
            with patch('celery.apps.worker.active_thread_count') as c:
                c.return_value = 3
                worker = self._Worker()
                handlers = self.psig(
                    cd.install_worker_term_hard_handler, worker)
                try:
                    handlers['SIGQUIT']('SIGQUIT', object())
                    assert state.should_terminate
                finally:
                    state.should_terminate = None
            with patch('celery.apps.worker.active_thread_count') as c:
                c.return_value = 1
                worker = self._Worker()
                handlers = self.psig(
                    cd.install_worker_term_hard_handler, worker)
                try:
                    with pytest.raises(WorkerTerminate):
                        handlers['SIGQUIT']('SIGQUIT', object())
                finally:
                    state.should_terminate = None
        finally:
            process.name = name

    def test_worker_term_handler_when_threads(self):
        with patch('celery.apps.worker.active_thread_count') as c:
            c.return_value = 3
            worker = self._Worker()
            handlers = self.psig(cd.install_worker_term_handler, worker)
            try:
                handlers['SIGTERM']('SIGTERM', object())
                assert state.should_stop == EX_OK
            finally:
                state.should_stop = None

    def test_worker_term_handler_when_single_thread(self):
        with patch('celery.apps.worker.active_thread_count') as c:
            c.return_value = 1
            worker = self._Worker()
            handlers = self.psig(cd.install_worker_term_handler, worker)
            try:
                with pytest.raises(WorkerShutdown):
                    handlers['SIGTERM']('SIGTERM', object())
            finally:
                state.should_stop = None

    @patch('sys.__stderr__')
    @skip.if_pypy()
    @skip.if_jython()
    def test_worker_cry_handler(self, stderr):
        handlers = self.psig(cd.install_cry_handler)
        assert handlers['SIGUSR1']('SIGUSR1', object()) is None
        stderr.write.assert_called()

    @skip.unless_module('multiprocessing')
    def test_worker_term_handler_only_stop_MainProcess(self):
        process = current_process()
        name, process.name = process.name, 'OtherProcess'
        try:
            with patch('celery.apps.worker.active_thread_count') as c:
                c.return_value = 3
                worker = self._Worker()
                handlers = self.psig(cd.install_worker_term_handler, worker)
                handlers['SIGTERM']('SIGTERM', object())
                assert state.should_stop == EX_OK
            with patch('celery.apps.worker.active_thread_count') as c:
                c.return_value = 1
                worker = self._Worker()
                handlers = self.psig(cd.install_worker_term_handler, worker)
                with pytest.raises(WorkerShutdown):
                    handlers['SIGTERM']('SIGTERM', object())
        finally:
            process.name = name
            state.should_stop = None

    @skip.unless_symbol('os.execv')
    @patch('celery.platforms.close_open_fds')
    @patch('atexit.register')
    @patch('os.close')
    def test_worker_restart_handler(self, _close, register, close_open):
        argv = []

        def _execv(*args):
            argv.extend(args)

        execv, os.execv = os.execv, _execv
        try:
            worker = self._Worker()
            handlers = self.psig(cd.install_worker_restart_handler, worker)
            handlers['SIGHUP']('SIGHUP', object())
            assert state.should_stop == EX_OK
            register.assert_called()
            callback = register.call_args[0][0]
            callback()
            assert argv
        finally:
            os.execv = execv
            state.should_stop = None

    def test_worker_term_hard_handler_when_threaded(self):
        with patch('celery.apps.worker.active_thread_count') as c:
            c.return_value = 3
            worker = self._Worker()
            handlers = self.psig(cd.install_worker_term_hard_handler, worker)
            try:
                handlers['SIGQUIT']('SIGQUIT', object())
                assert state.should_terminate
            finally:
                state.should_terminate = None

    def test_worker_term_hard_handler_when_single_threaded(self):
        with patch('celery.apps.worker.active_thread_count') as c:
            c.return_value = 1
            worker = self._Worker()
            handlers = self.psig(cd.install_worker_term_hard_handler, worker)
            with pytest.raises(WorkerTerminate):
                handlers['SIGQUIT']('SIGQUIT', object())

    def test_send_worker_shutting_down_signal(self):
        with patch('celery.apps.worker.signals.worker_shutting_down') as wsd:
            worker = self._Worker()
            handlers = self.psig(cd.install_worker_term_handler, worker)
            try:
                with pytest.raises(WorkerShutdown):
                    handlers['SIGTERM']('SIGTERM', object())
            finally:
                state.should_stop = None
            wsd.send.assert_called_with(
                sender='foo', sig='SIGTERM', how='Warm', exitcode=0,
            )
