from __future__ import absolute_import
from __future__ import with_statement

import sys
import logging
from tempfile import mktemp

from celery import log
from celery.log import (setup_logger, setup_task_logger,
                        get_default_logger, get_task_logger,
                        redirect_stdouts_to_logger, LoggingProxy,
                        setup_logging_subsystem)
from celery.utils import uuid
from celery.utils.compat import _CompatLoggerAdapter
from celery.tests.utils import (Case, override_stdouts, wrap_logger,
                                get_handlers, set_handlers)


class test_default_logger(Case):

    def setUp(self):
        self.setup_logger = setup_logger
        self.get_logger = get_default_logger
        log._setup = False

    def test_setup_logging_subsystem_colorize(self):
        setup_logging_subsystem(colorize=None)
        setup_logging_subsystem(colorize=True)

    def test_setup_logging_subsystem_no_mputil(self):
        mputil, log.mputil = log.mputil, None
        log.mputil
        try:
            log.setup_logging_subsystem()
        finally:
            log.mputil = mputil

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
        set_handlers(logger, [])
        logger = self.setup_logger(loglevel=logging.ERROR, logfile=None,
                                   root=False, colorize=None)
        self.assertIs(get_handlers(logger)[0].stream, sys.__stderr__,
                "setup_logger logs to stderr without logfile argument.")
        self.assertDidLogFalse(logger, "Logging something",
                "Logger doesn't info when loglevel is ERROR",
                loglevel=logging.INFO)

    def test_setup_logger_no_handlers_stream(self):
        l = self.get_logger()
        set_handlers(l, [])

        with override_stdouts() as outs:
            stdout, stderr = outs
            l = self.setup_logger(logfile=stderr, loglevel=logging.INFO,
                                  root=False)
            l.info("The quick brown fox...")
            self.assertIn("The quick brown fox...", stderr.getvalue())

    def test_setup_logger_no_handlers_file(self):
        l = self.get_logger()
        set_handlers(l, [])
        tempfile = mktemp(suffix="unittest", prefix="celery")
        l = self.setup_logger(logfile=tempfile, loglevel=0, root=False)
        self.assertIsInstance(get_handlers(l)[0],
                              logging.FileHandler)

    def test_redirect_stdouts(self):
        logger = self.setup_logger(loglevel=logging.ERROR, logfile=None,
                                   root=False)
        try:
            with wrap_logger(logger) as sio:
                redirect_stdouts_to_logger(logger, loglevel=logging.ERROR)
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
        logger = get_task_logger()
        logger.handlers = []
        logging.root.manager.loggerDict.pop(logger.name, None)
        self.uid = uuid()

    def setup_logger(self, *args, **kwargs):
        return setup_task_logger(*args, **dict(kwargs, task_name=self.uid,
                                                       task_id=self.uid))

    def get_logger(self, *args, **kwargs):
        return get_task_logger(*args, **dict(kwargs, name=self.uid))


class MockLogger(logging.Logger):
    _records = None

    def __init__(self, *args, **kwargs):
        self._records = []
        logging.Logger.__init__(self, *args, **kwargs)

    def handle(self, record):
        self._records.append(record)

    def isEnabledFor(self, level):
        return True


class test_CompatLoggerAdapter(Case):
    levels = ("debug",
              "info",
              "warn", "warning",
              "error",
              "fatal", "critical")

    def setUp(self):
        self.logger, self.adapter = self.createAdapter()

    def createAdapter(self, name=None, extra={"foo": "bar"}):
        logger = MockLogger(name=name or uuid())
        return logger, _CompatLoggerAdapter(logger, extra)

    def test_levels(self):
        for level in self.levels:
            msg = "foo bar %s" % (level, )
            logger, adapter = self.createAdapter()
            getattr(adapter, level)(msg)
            self.assertEqual(logger._records[0].msg, msg)

    def test_exception(self):
        try:
            raise KeyError("foo")
        except KeyError:
            self.adapter.exception("foo bar exception")
        self.assertEqual(self.logger._records[0].msg, "foo bar exception")

    def test_setLevel(self):
        self.adapter.setLevel(logging.INFO)
        self.assertEqual(self.logger.level, logging.INFO)

    def test_process(self):
        msg, kwargs = self.adapter.process("foo bar baz", {"exc_info": 1})
        self.assertDictEqual(kwargs, {"exc_info": 1,
                                      "extra": {"foo": "bar"}})

    def test_add_remove_handlers(self):
        handler = logging.StreamHandler()
        self.adapter.addHandler(handler)
        self.assertIs(self.logger.handlers[0], handler)
        self.adapter.removeHandler(handler)
        self.assertListEqual(self.logger.handlers, [])
