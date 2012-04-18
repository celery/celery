from __future__ import absolute_import
from __future__ import with_statement

import sys
import logging
from tempfile import mktemp

from celery import current_app
from celery.app.log import Logging
from celery.utils.log import LoggingProxy
from celery.utils import uuid
from celery.utils.log import get_logger
from celery.tests.utils import (
    Case, override_stdouts, wrap_logger, get_handlers,
)

log = current_app.log


class test_default_logger(Case):

    def setUp(self):
        self.setup_logger = log.setup_logger
        self.get_logger = lambda n=None: get_logger(n) if n else logging.root
        Logging._setup = False

    def test_setup_logging_subsystem_colorize(self):
        log.setup_logging_subsystem(colorize=None)
        log.setup_logging_subsystem(colorize=True)

    def test_setup_logging_subsystem_no_mputil(self):
        from celery.utils import log as logtools
        mputil, logtools.mputil = logtools.mputil, None
        try:
            log.setup_logging_subsystem()
        finally:
            logtools.mputil = mputil

    def _assertLog(self, logger, logmsg, loglevel=logging.ERROR):

        with wrap_logger(logger, loglevel=loglevel) as sio:
            logger.log(loglevel, logmsg)
            return sio.getvalue().strip()

    def assertDidLogTrue(self, logger, logmsg, reason, loglevel=None):
        val = self._assertLog(logger, logmsg, loglevel=loglevel)
        return self.assertEqual(val, logmsg, reason)

    def assertDidLogFalse(self, logger, logmsg, reason, loglevel=None):
        val = self._assertLog(logger, logmsg, loglevel=loglevel)
        return self.assertFalse(val, reason)

    def test_setup_logger(self):
        logger = self.setup_logger(loglevel=logging.ERROR, logfile=None,
                                   root=False, colorize=True)
        logger.handlers = []
        Logging._setup = False
        logger = self.setup_logger(loglevel=logging.ERROR, logfile=None,
                                   root=False, colorize=None)
        print(logger.handlers)
        self.assertIs(get_handlers(logger)[0].stream, sys.__stderr__,
                "setup_logger logs to stderr without logfile argument.")

    def test_setup_logger_no_handlers_stream(self):
        l = self.get_logger()
        l.handlers = []

        with override_stdouts() as outs:
            stdout, stderr = outs
            l = self.setup_logger(logfile=sys.stderr, loglevel=logging.INFO,
                                root=False)
            l.info("The quick brown fox...")
            self.assertIn("The quick brown fox...", stderr.getvalue())

    def test_setup_logger_no_handlers_file(self):
        l = self.get_logger()
        l.handlers = []
        tempfile = mktemp(suffix="unittest", prefix="celery")
        l = self.setup_logger(logfile=tempfile, loglevel=0, root=False)
        self.assertIsInstance(get_handlers(l)[0],
                              logging.FileHandler)

    def test_redirect_stdouts(self):
        logger = self.setup_logger(loglevel=logging.ERROR, logfile=None,
                                   root=False)
        try:
            with wrap_logger(logger) as sio:
                log.redirect_stdouts_to_logger(logger, loglevel=logging.ERROR)
                logger.error("foo")
                self.assertIn("foo", sio.getvalue())
        finally:
            sys.stdout, sys.stderr = sys.__stdout__, sys.__stderr__

    def test_logging_proxy(self):
        logger = self.setup_logger(loglevel=logging.ERROR, logfile=None,
                                   root=False)

        with wrap_logger(logger) as sio:
            p = LoggingProxy(logger, loglevel=logging.ERROR)
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


class test_task_logger(test_default_logger):

    def setUp(self):
        logger = self.logger = get_logger("celery.task")
        logger.handlers = []
        logging.root.manager.loggerDict.pop(logger.name, None)
        self.uid = uuid()

        @current_app.task
        def test_task():
            pass
        test_task.logger.handlers = []
        self.task = test_task
        from celery.app.state import _tls
        _tls.current_task = test_task

    def tearDown(self):
        from celery.app.state import _tls
        _tls.current_task = None

    def setup_logger(self, *args, **kwargs):
        return log.setup_task_loggers(*args, **kwargs)

    def get_logger(self, *args, **kwargs):
        return self.task.logger


class MockLogger(logging.Logger):
    _records = None

    def __init__(self, *args, **kwargs):
        self._records = []
        logging.Logger.__init__(self, *args, **kwargs)

    def handle(self, record):
        self._records.append(record)

    def isEnabledFor(self, level):
        return True
