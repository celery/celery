from __future__ import generators

import os
import sys
import logging
import unittest2 as unittest
from tempfile import mktemp
from StringIO import StringIO

try:
    from contextlib import contextmanager
except ImportError:
    from celery.tests.utils import fallback_contextmanager as contextmanager

from carrot.utils import rpartition

from celery.log import (setup_logger, emergency_error,
                        redirect_stdouts_to_logger, LoggingProxy)
from celery.tests.utils import override_stdouts, execute_context


@contextmanager
def wrap_logger(logger, loglevel=logging.ERROR):
    old_handlers = logger.handlers
    sio = StringIO()
    siohandler = logging.StreamHandler(sio)
    logger.handlers = [siohandler]

    yield sio

    logger.handlers = old_handlers


class TestLog(unittest.TestCase):

    def _assertLog(self, logger, logmsg, loglevel=logging.ERROR):

        def with_wrap_logger(sio):
            logger.log(loglevel, logmsg)
            return sio.getvalue().strip()

        context = wrap_logger(logger, loglevel=loglevel)
        execute_context(context, with_wrap_logger)

    def assertDidLogTrue(self, logger, logmsg, reason, loglevel=None):
        val = self._assertLog(logger, logmsg, loglevel=loglevel)
        return self.assertEqual(val, logmsg, reason)

    def assertDidLogFalse(self, logger, logmsg, reason, loglevel=None):
        val = self._assertLog(logger, logmsg, loglevel=loglevel)
        return self.assertFalse(val, reason)

    def test_setup_logger(self):
        logger = setup_logger(loglevel=logging.ERROR, logfile=None)
        logger.handlers = [] # Reset previously set logger.
        logger = setup_logger(loglevel=logging.ERROR, logfile=None)
        self.assertIs(logger.handlers[0].stream, sys.__stderr__,
                "setup_logger logs to stderr without logfile argument.")
        self.assertDidLogFalse(logger, "Logging something",
                "Logger doesn't info when loglevel is ERROR",
                loglevel=logging.INFO)

    def test_emergency_error(self):
        sio = StringIO()
        emergency_error(sio, "Testing emergency error facility")
        self.assertEqual(rpartition(sio.getvalue(), ":")[2].strip(),
                             "Testing emergency error facility")

    def test_setup_logger_no_handlers_stream(self):
        from multiprocessing import get_logger
        l = get_logger()
        l.handlers = []

        def with_override_stdouts(outs):
            stdout, stderr = outs
            l = setup_logger(logfile=stderr, loglevel=logging.INFO)
            l.info("The quick brown fox...")
            self.assertIn("The quick brown fox...", stderr.getvalue())

        context = override_stdouts()
        execute_context(context, with_override_stdouts)

    def test_setup_logger_no_handlers_file(self):
        from multiprocessing import get_logger
        l = get_logger()
        l.handlers = []
        tempfile = mktemp(suffix="unittest", prefix="celery")
        l = setup_logger(logfile=tempfile, loglevel=0)
        self.assertIsInstance(l.handlers[0], logging.FileHandler)

    def test_emergency_error_stderr(self):
        def with_override_stdouts(outs):
            stdout, stderr = outs
            emergency_error(None, "The lazy dog crawls under the fast fox")
            self.assertIn("The lazy dog crawls under the fast fox",
                          stderr.getvalue())

        context = override_stdouts()
        execute_context(context, with_override_stdouts)

    def test_emergency_error_file(self):
        tempfile = mktemp(suffix="unittest", prefix="celery")
        emergency_error(tempfile, "Vandelay Industries")
        tempfilefh = open(tempfile, "r")
        try:
            self.assertIn("Vandelay Industries", "".join(tempfilefh))
        finally:
            tempfilefh.close()
            os.unlink(tempfile)

    def test_redirect_stdouts(self):
        logger = setup_logger(loglevel=logging.ERROR, logfile=None)
        try:
            def with_wrap_logger(sio):
                redirect_stdouts_to_logger(logger, loglevel=logging.ERROR)
                logger.error("foo")
                self.assertIn("foo", sio.getvalue())

            context = wrap_logger(logger)
            execute_context(context, with_wrap_logger)
        finally:
            sys.stdout, sys.stderr = sys.__stdout__, sys.__stderr__

    def test_logging_proxy(self):
        logger = setup_logger(loglevel=logging.ERROR, logfile=None)

        def with_wrap_logger(sio):
            p = LoggingProxy(logger)
            p.close()
            p.write("foo")
            self.assertNotIn("foo", sio.getvalue())
            p.closed = False
            p.write("foo")
            self.assertIn("foo", sio.getvalue())
            lines = ["baz", "xuzzy"]
            p.writelines(lines)
            for line in lines:
                self.assertIn(line, sio.getvalue())
            p.flush()
            p.close()
            self.assertFalse(p.isatty())
            self.assertIsNone(p.fileno())

        context = wrap_logger(logger)
        execute_context(context, with_wrap_logger)
