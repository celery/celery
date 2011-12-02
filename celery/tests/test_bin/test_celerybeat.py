from __future__ import absolute_import
from __future__ import with_statement

import logging
import sys

from collections import defaultdict

from kombu.tests.utils import redirect_stdouts

from celery import beat
from celery import platforms
from celery.app import app_or_default
from celery.bin import celerybeat as celerybeat_bin
from celery.apps import beat as beatapp

from celery.tests.utils import AppCase


class MockedShelveModule(object):
    shelves = defaultdict(lambda: {})

    def open(self, filename, *args, **kwargs):
        return self.shelves[filename]
mocked_shelve = MockedShelveModule()


class MockService(beat.Service):
    started = False
    in_sync = False
    persistence = mocked_shelve

    def start(self):
        self.__class__.started = True

    def sync(self):
        self.__class__.in_sync = True


class MockBeat(beatapp.Beat):
    running = False

    def run(self):
        self.__class__.running = True


class MockBeat2(beatapp.Beat):
    Service = MockService

    def install_sync_handler(self, b):
        pass


class MockBeat3(beatapp.Beat):
    Service = MockService

    def install_sync_handler(self, b):
        raise TypeError("xxx")


class test_Beat(AppCase):

    def test_loglevel_string(self):
        b = beatapp.Beat(loglevel="DEBUG")
        self.assertEqual(b.loglevel, logging.DEBUG)

        b2 = beatapp.Beat(loglevel=logging.DEBUG)
        self.assertEqual(b2.loglevel, logging.DEBUG)

    def test_init_loader(self):
        b = beatapp.Beat()
        b.init_loader()

    def test_process_title(self):
        b = beatapp.Beat()
        b.set_process_title()

    def test_run(self):
        b = MockBeat2()
        MockService.started = False
        b.run()
        self.assertTrue(MockService.started)

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

    def test_install_sync_handler(self):
        b = beatapp.Beat()
        clock = MockService()
        MockService.in_sync = False
        handlers = self.psig(b.install_sync_handler, clock)
        with self.assertRaises(SystemExit):
            handlers["SIGINT"]("SIGINT", object())
        self.assertTrue(MockService.in_sync)
        MockService.in_sync = False

    def test_setup_logging(self):
        try:
            # py3k
            delattr(sys.stdout, "logger")
        except AttributeError:
            pass
        b = beatapp.Beat()
        b.redirect_stdouts = False
        b.setup_logging()
        with self.assertRaises(AttributeError):
            sys.stdout.logger

    @redirect_stdouts
    def test_logs_errors(self, stdout, stderr):
        class MockLogger(object):
            _critical = []

            def debug(self, *args, **kwargs):
                pass

            def critical(self, msg, *args, **kwargs):
                self._critical.append(msg)

        logger = MockLogger()
        b = MockBeat3(socket_timeout=None)
        b.start_scheduler(logger)

        self.assertTrue(logger._critical)

    @redirect_stdouts
    def test_use_pidfile(self, stdout, stderr):
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
            b = MockBeat2(pidfile="pidfilelockfilepid", socket_timeout=None)
            b.start_scheduler()
            self.assertTrue(create_pidlock.instance[0].acquired)
        finally:
            platforms.create_pidlock = prev


class MockDaemonContext(object):
    opened = False
    closed = False

    def __init__(self, *args, **kwargs):
        pass

    def open(self):
        self.__class__.opened = True
        return self
    __enter__ = open

    def close(self, *args):
        self.__class__.closed = True
    __exit__ = close


class test_div(AppCase):

    def setup(self):
        self.prev, beatapp.Beat = beatapp.Beat, MockBeat
        self.ctx, celerybeat_bin.detached = \
                celerybeat_bin.detached, MockDaemonContext

    def teardown(self):
        beatapp.Beat = self.prev

    def test_main(self):
        sys.argv = [sys.argv[0], "-s", "foo"]
        try:
            celerybeat_bin.main()
            self.assertTrue(MockBeat.running)
        finally:
            MockBeat.running = False

    def test_detach(self):
        cmd = celerybeat_bin.BeatCommand()
        cmd.app = app_or_default()
        cmd.run(detach=True)
        self.assertTrue(MockDaemonContext.opened)
        self.assertTrue(MockDaemonContext.closed)

    def test_parse_options(self):
        cmd = celerybeat_bin.BeatCommand()
        cmd.app = app_or_default()
        options, args = cmd.parse_options("celerybeat", ["-s", "foo"])
        self.assertEqual(options.schedule, "foo")
