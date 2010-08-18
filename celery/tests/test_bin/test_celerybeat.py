import logging
import sys
import unittest2 as unittest

from celery import beat
from celery import platform
from celery.bin import celerybeat as celerybeat


class MockService(beat.Service):
    started = False
    in_sync = False

    def start(self):
        self.__class__.started = True

    def sync(self):
        self.__class__.in_sync = True


class MockBeat(celerybeat.Beat):
    running = False

    def run(self):
        self.__class__.running = True


class MockBeat2(celerybeat.Beat):
    Service = MockService

    def install_sync_handler(self, b):
        pass


class test_Beat(unittest.TestCase):

    def test_loglevel_string(self):
        b = celerybeat.Beat(loglevel="DEBUG")
        self.assertEqual(b.loglevel, logging.DEBUG)

        b2 = celerybeat.Beat(loglevel=logging.DEBUG)
        self.assertEqual(b2.loglevel, logging.DEBUG)

    def test_init_loader(self):
        b = celerybeat.Beat()
        b.init_loader()

    def test_startup_info(self):
        b = celerybeat.Beat()
        self.assertIn("@stderr", b.startup_info())

    def test_process_title(self):
        b = celerybeat.Beat()
        b.set_process_title()

    def test_run(self):
        b = MockBeat2()
        MockService.started = False
        b.run()
        self.assertTrue(MockService.started)

    def psig(self, fun, *args, **kwargs):
        handlers = {}

        def i(sig, handler):
            handlers[sig] = handler

        p, platform.install_signal_handler = platform.install_signal_handler, i
        try:
            fun(*args, **kwargs)
            return handlers
        finally:
            platform.install_signal_handler = p

    def test_install_sync_handler(self):
        b = celerybeat.Beat()
        clock = MockService()
        MockService.in_sync = False
        handlers = self.psig(b.install_sync_handler, clock)
        self.assertRaises(SystemExit, handlers["SIGINT"],
                          "SIGINT", object())
        self.assertTrue(MockService.in_sync)
        MockService.in_sync = False


class test_div(unittest.TestCase):

    def setUp(self):
        self.prev, celerybeat.Beat = celerybeat.Beat, MockBeat

    def tearDown(self):
        celerybeat.Beat = self.prev

    def test_main(self):
        sys.argv = [sys.argv[0], "-s", "foo"]
        try:
            celerybeat.main()
            self.assertTrue(MockBeat.running)
        finally:
            MockBeat.running = False

    def test_run_celerybeat(self):
        try:
            celerybeat.run_celerybeat()
            self.assertTrue(MockBeat.running)
        finally:
            MockBeat.running = False

    def test_parse_options(self):
        options = celerybeat.parse_options(["-s", "foo"])
        self.assertEqual(options.schedule, "foo")
