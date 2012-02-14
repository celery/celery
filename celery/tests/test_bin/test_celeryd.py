from __future__ import absolute_import
from __future__ import with_statement

import logging
import os
import sys

from functools import wraps
try:
    from multiprocessing import current_process
except ImportError:
    current_process = None  # noqa

from mock import patch
from nose import SkipTest

from celery import Celery
from celery import platforms
from celery import signals
from celery import current_app
from celery.apps import worker as cd
from celery.bin.celeryd import WorkerCommand, main as celeryd_main
from celery.exceptions import ImproperlyConfigured

from celery.tests.utils import (AppCase, WhateverIO, mask_modules,
                                reset_modules, skip_unless_module)


from celery.utils.patch import ensure_process_aware_logger
ensure_process_aware_logger()


def disable_stdouts(fun):

    @wraps(fun)
    def disable(*args, **kwargs):
        sys.stdout, sys.stderr = WhateverIO(), WhateverIO()
        try:
            return fun(*args, **kwargs)
        finally:
            sys.stdout = sys.__stdout__
            sys.stderr = sys.__stderr__

    return disable


class _WorkController(object):

    def __init__(self, *args, **kwargs):
        self.logger = current_app.log.get_default_logger()

    def start(self):
        pass


class Worker(cd.Worker):
    WorkController = _WorkController


class test_compilation(AppCase):

    def test_no_multiprocessing(self):
        with mask_modules("multiprocessing"):
            with reset_modules("celery.apps.worker"):
                from celery.apps.worker import multiprocessing
                self.assertIsNone(multiprocessing)

    def test_cpu_count_no_mp(self):
        with mask_modules("multiprocessing"):
            with reset_modules("celery.apps.worker"):
                from celery.apps.worker import cpu_count
                self.assertEqual(cpu_count(), 2)

    @skip_unless_module("multiprocessing")
    def test_no_cpu_count(self):

        @patch("multiprocessing.cpu_count")
        def _do_test(pcount):
            pcount.side_effect = NotImplementedError("cpu_count")
            from celery.apps.worker import cpu_count
            self.assertEqual(cpu_count(), 2)
            pcount.assert_called_with()

        _do_test()

    def test_process_name_wo_mp(self):
        with mask_modules("multiprocessing"):
            with reset_modules("celery.apps.worker"):
                from celery.apps.worker import get_process_name
                self.assertIsNone(get_process_name())

    @skip_unless_module("multiprocessing")
    def test_process_name_w_mp(self):

        @patch("multiprocessing.current_process")
        def _do_test(current_process):
            from celery.apps.worker import get_process_name
            self.assertTrue(get_process_name())

        _do_test()


class test_Worker(AppCase):
    Worker = Worker

    @disable_stdouts
    def test_queues_string(self):
        celery = Celery(set_as_current=False)
        worker = celery.Worker(queues="foo,bar,baz")
        worker.init_queues()
        self.assertEqual(worker.use_queues, ["foo", "bar", "baz"])
        self.assertTrue("foo" in celery.amqp.queues)

    @disable_stdouts
    def test_windows_B_option(self):
        celery = Celery(set_as_current=False)
        celery.IS_WINDOWS = True
        with self.assertRaises(SystemExit):
            celery.Worker(embed_clockservice=True)

    def test_tasklist(self):
        celery = Celery(set_as_current=False)
        worker = celery.Worker()
        self.assertTrue(worker.tasklist(include_builtins=True))
        worker.tasklist(include_builtins=False)

    def test_extra_info(self):
        celery = Celery(set_as_current=False)
        worker = celery.Worker()
        worker.loglevel = logging.WARNING
        self.assertFalse(worker.extra_info())
        worker.loglevel = logging.INFO
        self.assertTrue(worker.extra_info())

    @disable_stdouts
    def test_loglevel_string(self):
        worker = self.Worker(loglevel="INFO")
        self.assertEqual(worker.loglevel, logging.INFO)

    def test_run_worker(self):
        handlers = {}

        class Signals(platforms.Signals):

            def __setitem__(self, sig, handler):
                handlers[sig] = handler

        p = platforms.signals
        platforms.signals = Signals()
        try:
            w = self.Worker()
            w._isatty = False
            w.run_worker()
            for sig in "SIGINT", "SIGHUP", "SIGTERM":
                self.assertIn(sig, handlers)

            handlers.clear()
            w = self.Worker()
            w._isatty = True
            w.run_worker()
            for sig in "SIGINT", "SIGTERM":
                self.assertIn(sig, handlers)
            self.assertNotIn("SIGHUP", handlers)
        finally:
            platforms.signals = p

    @disable_stdouts
    def test_startup_info(self):
        worker = self.Worker()
        worker.run()
        self.assertTrue(worker.startup_info())
        worker.loglevel = logging.DEBUG
        self.assertTrue(worker.startup_info())
        worker.loglevel = logging.INFO
        self.assertTrue(worker.startup_info())
        worker.autoscale = 13, 10
        self.assertTrue(worker.startup_info())

    @disable_stdouts
    def test_run(self):
        self.Worker().run()
        self.Worker(discard=True).run()
        worker = self.Worker()
        worker.init_loader()
        worker.run()

    @disable_stdouts
    def test_purge_messages(self):
        self.Worker().purge_messages()

    @disable_stdouts
    def test_init_queues(self):
        app = current_app
        c = app.conf
        p, app.amqp.queues = app.amqp.queues, app.amqp.Queues({
                "celery": {"exchange": "celery",
                           "binding_key": "celery"},
                "video": {"exchange": "video",
                           "binding_key": "video"}})
        try:
            worker = self.Worker(queues=["video"])
            worker.init_queues()
            self.assertIn("video", app.amqp.queues)
            self.assertIn("video", app.amqp.queues.consume_from)
            self.assertIn("celery", app.amqp.queues)
            self.assertNotIn("celery", app.amqp.queues.consume_from)

            c.CELERY_CREATE_MISSING_QUEUES = False
            with self.assertRaises(ImproperlyConfigured):
                self.Worker(queues=["image"]).init_queues()
            c.CELERY_CREATE_MISSING_QUEUES = True
            worker = self.Worker(queues=["image"])
            worker.init_queues()
            self.assertIn("image", app.amqp.queues.consume_from)
            self.assertDictContainsSubset({"exchange": "image",
                                           "routing_key": "image",
                                           "binding_key": "image",
                                           "exchange_type": "direct"},
                                            app.amqp.queues["image"])
        finally:
            app.amqp.queues = p

    @disable_stdouts
    def test_autoscale_argument(self):
        worker1 = self.Worker(autoscale="10,3")
        self.assertListEqual(worker1.autoscale, [10, 3])
        worker2 = self.Worker(autoscale="10")
        self.assertListEqual(worker2.autoscale, [10, 0])

    def test_include_argument(self):
        worker1 = self.Worker(include="some.module")
        self.assertListEqual(worker1.include, ["some.module"])
        worker2 = self.Worker(include="some.module,another.package")
        self.assertListEqual(worker2.include, ["some.module",
                                               "another.package"])
        worker3 = self.Worker(include="os,sys")
        worker3.init_loader()

    @disable_stdouts
    def test_unknown_loglevel(self):
        with self.assertRaises(SystemExit):
            self.Worker(loglevel="ALIEN")
        worker1 = self.Worker(loglevel=0xFFFF)
        self.assertEqual(worker1.loglevel, 0xFFFF)

    def test_warns_if_running_as_privileged_user(self):
        app = current_app
        if app.IS_WINDOWS:
            raise SkipTest("Not applicable on Windows")

        def getuid():
            return 0

        prev, os.getuid = os.getuid, getuid
        try:
            with self.assertWarnsRegex(RuntimeWarning,
                    r'superuser privileges is discouraged'):
                worker = self.Worker()
                worker.run()
        finally:
            os.getuid = prev

    @disable_stdouts
    def test_use_pidfile(self):
        from celery import platforms

        class create_pidlock(object):
            instance = [None]

            def __init__(self, file):
                self.file = file
                self.instance[0] = self

            def acquire(self):
                self.acquired = True

                class Object(object):
                    def release(self):
                        pass

                return Object()

        prev, platforms.create_pidlock = platforms.create_pidlock, \
                                         create_pidlock
        try:
            worker = self.Worker(pidfile="pidfilelockfilepid")
            worker.run_worker()
            self.assertTrue(create_pidlock.instance[0].acquired)
        finally:
            platforms.create_pidlock = prev

    @disable_stdouts
    def test_redirect_stdouts(self):
        worker = self.Worker()
        worker.redirect_stdouts = False
        worker.redirect_stdouts_to_logger()
        with self.assertRaises(AttributeError):
            sys.stdout.logger

    def test_redirect_stdouts_already_handled(self):
        logging_setup = [False]

        def on_logging_setup(**kwargs):
            logging_setup[0] = True

        signals.setup_logging.connect(on_logging_setup)
        try:
            worker = self.Worker()
            worker.app.log.__class__._setup = False
            worker.redirect_stdouts_to_logger()
            self.assertTrue(logging_setup[0])
            with self.assertRaises(AttributeError):
                sys.stdout.logger
        finally:
            signals.setup_logging.disconnect(on_logging_setup)

    @disable_stdouts
    def test_platform_tweaks_osx(self):

        class OSXWorker(self.Worker):
            proxy_workaround_installed = False

            def osx_proxy_detection_workaround(self):
                self.proxy_workaround_installed = True

        worker = OSXWorker()

        def install_HUP_nosupport(controller):
            controller.hup_not_supported_installed = True

        class Controller(object):
            logger = logging.getLogger("celery.tests")

        prev = cd.install_HUP_not_supported_handler
        cd.install_HUP_not_supported_handler = install_HUP_nosupport
        try:
            worker.app.IS_OSX = True
            controller = Controller()
            worker.install_platform_tweaks(controller)
            self.assertTrue(controller.hup_not_supported_installed)
            self.assertTrue(worker.proxy_workaround_installed)
        finally:
            cd.install_HUP_not_supported_handler = prev

    @disable_stdouts
    def test_general_platform_tweaks(self):

        restart_worker_handler_installed = [False]

        def install_worker_restart_handler(worker):
            restart_worker_handler_installed[0] = True

        class Controller(object):
            logger = logging.getLogger("celery.tests")

        prev = cd.install_worker_restart_handler
        cd.install_worker_restart_handler = install_worker_restart_handler
        try:
            worker = self.Worker()
            worker.app.IS_OSX = False
            worker.install_platform_tweaks(Controller())
            self.assertTrue(restart_worker_handler_installed[0])
        finally:
            cd.install_worker_restart_handler = prev

    @disable_stdouts
    def test_on_consumer_ready(self):
        worker_ready_sent = [False]

        def on_worker_ready(**kwargs):
            worker_ready_sent[0] = True

        signals.worker_ready.connect(on_worker_ready)

        self.Worker().on_consumer_ready(object())
        self.assertTrue(worker_ready_sent[0])


class test_funs(AppCase):

    @disable_stdouts
    def test_set_process_status(self):
        try:
            __import__("setproctitle")
        except ImportError:
            raise SkipTest("setproctitle not installed")
        worker = Worker(hostname="xyzza")
        prev1, sys.argv = sys.argv, ["Arg0"]
        try:
            st = worker.set_process_status("Running")
            self.assertIn("celeryd", st)
            self.assertIn("xyzza", st)
            self.assertIn("Running", st)
            prev2, sys.argv = sys.argv, ["Arg0", "Arg1"]
            try:
                st = worker.set_process_status("Running")
                self.assertIn("celeryd", st)
                self.assertIn("xyzza", st)
                self.assertIn("Running", st)
                self.assertIn("Arg1", st)
            finally:
                sys.argv = prev2
        finally:
            sys.argv = prev1

    @disable_stdouts
    def test_parse_options(self):
        cmd = WorkerCommand()
        cmd.app = current_app
        opts, args = cmd.parse_options("celeryd", ["--concurrency=512"])
        self.assertEqual(opts.concurrency, 512)

    @disable_stdouts
    def test_main(self):
        p, cd.Worker = cd.Worker, Worker
        s, sys.argv = sys.argv, ["celeryd", "--discard"]
        try:
            celeryd_main()
        finally:
            cd.Worker = p
            sys.argv = s


class test_signal_handlers(AppCase):

    class _Worker(object):
        stopped = False
        terminated = False
        logger = current_app.log.get_default_logger()

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

    @disable_stdouts
    def test_worker_int_handler(self):
        worker = self._Worker()
        handlers = self.psig(cd.install_worker_int_handler, worker)
        next_handlers = {}

        class Signals(platforms.Signals):

            def __setitem__(self, sig, handler):
                next_handlers[sig] = handler

        p, platforms.signals = platforms.signals, Signals()
        try:
            with self.assertRaises(SystemExit):
                handlers["SIGINT"]("SIGINT", object())
            self.assertTrue(worker.stopped)
        finally:
            platforms.signals = p

        with self.assertRaises(SystemExit):
            next_handlers["SIGINT"]("SIGINT", object())
        self.assertTrue(worker.terminated)

    @disable_stdouts
    def test_worker_int_handler_only_stop_MainProcess(self):
        if current_process is None:
            raise SkipTest("only relevant for multiprocessing")
        process = current_process()
        name, process.name = process.name, "OtherProcess"
        try:
            worker = self._Worker()
            handlers = self.psig(cd.install_worker_int_handler, worker)
            with self.assertRaises(SystemExit):
                handlers["SIGINT"]("SIGINT", object())
            self.assertFalse(worker.stopped)
        finally:
            process.name = name

    @disable_stdouts
    def test_install_HUP_not_supported_handler(self):
        worker = self._Worker()
        handlers = self.psig(cd.install_HUP_not_supported_handler, worker)
        handlers["SIGHUP"]("SIGHUP", object())

    @disable_stdouts
    def test_worker_int_again_handler_only_stop_MainProcess(self):
        if current_process is None:
            raise SkipTest("only relevant for multiprocessing")
        process = current_process()
        name, process.name = process.name, "OtherProcess"
        try:
            worker = self._Worker()
            handlers = self.psig(cd.install_worker_int_again_handler, worker)
            with self.assertRaises(SystemExit):
                handlers["SIGINT"]("SIGINT", object())
            self.assertFalse(worker.terminated)
        finally:
            process.name = name

    @disable_stdouts
    def test_worker_term_handler(self):
        worker = self._Worker()
        handlers = self.psig(cd.install_worker_term_handler, worker)
        with self.assertRaises(SystemExit):
            handlers["SIGTERM"]("SIGTERM", object())
        self.assertTrue(worker.stopped)

    def test_worker_cry_handler(self):
        if sys.platform.startswith("java"):
            raise SkipTest("Cry handler does not work on Jython")
        if hasattr(sys, "pypy_version_info"):
            raise SkipTest("Cry handler does not work on PyPy")
        if sys.version_info > (2, 5):

            class Logger(object):
                _errors = []

                def error(self, msg, *args, **kwargs):
                    self._errors.append(msg)
            logger = Logger()
            handlers = self.psig(cd.install_cry_handler, logger)
            self.assertIsNone(handlers["SIGUSR1"]("SIGUSR1", object()))
            self.assertTrue(Logger._errors)
        else:
            raise SkipTest("Needs Python 2.5 or later")

    @disable_stdouts
    def test_worker_term_handler_only_stop_MainProcess(self):
        if current_process is None:
            raise SkipTest("only relevant for multiprocessing")
        process = current_process()
        name, process.name = process.name, "OtherProcess"
        try:
            worker = self._Worker()
            handlers = self.psig(cd.install_worker_term_handler, worker)
            with self.assertRaises(SystemExit):
                handlers["SIGTERM"]("SIGTERM", object())
            self.assertFalse(worker.stopped)
        finally:
            process.name = name

    @disable_stdouts
    def test_worker_restart_handler(self):
        if getattr(os, "execv", None) is None:
            raise SkipTest("platform does not have excv")
        argv = []

        def _execv(*args):
            argv.extend(args)

        execv, os.execv = os.execv, _execv
        try:
            worker = self._Worker()
            handlers = self.psig(cd.install_worker_restart_handler, worker)
            handlers["SIGHUP"]("SIGHUP", object())
            self.assertTrue(worker.stopped)
            self.assertTrue(argv)
        finally:
            os.execv = execv


    @disable_stdouts
    def test_worker_term_hard_handler(self):
        worker = self._Worker()
        handlers = self.psig(cd.install_worker_term_hard_handler, worker)
        with self.assertRaises(SystemExit):
            handlers["SIGQUIT"]("SIGQUIT", object())
        self.assertTrue(worker.terminated)
