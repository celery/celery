from nose import SkipTest

from celery.app import app_or_default
from celery.bin import celeryev

from celery.tests.utils import unittest
from celery.tests.utils import patch


class MockCommand(object):
    executed = []

    def execute_from_commandline(self, **kwargs):
        self.executed.append(True)


def proctitle(prog, info=None):
    proctitle.last = (prog, info)
proctitle.last = ()


class test_EvCommand(unittest.TestCase):

    def setUp(self):
        self.app = app_or_default()
        self.ev = celeryev.EvCommand(app=self.app)

    @patch("celery.events.dumper", "evdump", lambda **kw: "me dumper, you?")
    @patch("celery.platforms", "set_process_title", proctitle)
    def test_run_dump(self):
        self.assertEqual(self.ev.run(dump=True), "me dumper, you?")
        self.assertIn("celeryev:dump", proctitle.last[0])

    def test_run_top(self):
        try:
            import curses
        except ImportError:
            raise SkipTest("curses monitor requires curses")

        @patch("celery.events.cursesmon", "evtop", lambda **kw: "me top, you?")
        @patch("celery.platforms", "set_process_title", proctitle)
        def _inner():
            self.assertEqual(self.ev.run(), "me top, you?")
            self.assertIn("celeryev:top", proctitle.last[0])
        return _inner()

    @patch("celery.events.snapshot", "evcam", lambda *a, **k: (a, k))
    @patch("celery.platforms", "set_process_title", proctitle)
    def test_run_cam(self):
        a, kw = self.ev.run(camera="foo.bar.baz", logfile="logfile")
        self.assertEqual(a[0], "foo.bar.baz")
        self.assertEqual(kw["freq"], 1.0)
        self.assertIsNone(kw["maxrate"])
        self.assertEqual(kw["loglevel"], "INFO")
        self.assertEqual(kw["logfile"], "logfile")
        self.assertIn("celeryev:cam", proctitle.last[0])

    @patch("celery.bin.celeryev", "EvCommand", MockCommand)
    def test_main(self):
        MockCommand.executed = []
        celeryev.main()
        self.assertTrue(MockCommand.executed)
