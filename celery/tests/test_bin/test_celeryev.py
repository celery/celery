import sys

from importlib import import_module

from celery.app import app_or_default
from celery.bin import celeryev
from celery.utils.functional import wraps

from celery.tests.utils import unittest


def patch(module, name, mocked):
    module = import_module(module)

    def _patch(fun):

        @wraps(fun)
        def __patched(*args, **kwargs):
            prev = getattr(module, name)
            setattr(module, name, mocked)
            try:
                return fun(*args, **kwargs)
            finally:
                setattr(module, name, prev)
        return __patched
    return _patch


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

    @patch("celery.events.cursesmon", "evtop", lambda **kw: "me top, you?")
    @patch("celery.platforms", "set_process_title", proctitle)
    def test_run_top(self):
        self.assertEqual(self.ev.run(), "me top, you?")
        self.assertIn("celeryev:top", proctitle.last[0])

    @patch("celery.events.snapshot", "evcam", lambda *a, **k: (a, k))
    @patch("celery.platforms", "set_process_title", proctitle)
    def test_run_cam(self):
        a, kw = self.ev.run(camera="foo.bar.baz", logfile="logfile")
        self.assertEqual(a[0], "foo.bar.baz")
        self.assertEqual(a[1], 1.0)
        self.assertIsNone(a[2])
        self.assertEqual(kw["loglevel"], "INFO")
        self.assertEqual(kw["logfile"], "logfile")
        self.assertIn("celeryev:cam", proctitle.last[0])

    @patch("celery.events.cursesmon", "evtop", lambda **kw: "me top, you?")
    @patch("celery.platforms", "set_process_title", proctitle)
    def test_main(self):
        prev_argv = list(sys.argv)
        sys.argv = ["celeryev"]
        try:
            celeryev.main()
            self.assertIn("celeryev:top", proctitle.last[0])
        finally:
            sys.argv = prev_argv

