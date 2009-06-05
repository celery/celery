import unittest

import sys
import logging
import multiprocessing
from StringIO import StringIO
from celery.log import setup_logger, emergency_error


class TestLog(unittest.TestCase):

    def _assertLog(self, logger, logmsg, loglevel=logging.ERROR):
        # Save old handlers
        old_handler = logger.handlers[0]
        logger.removeHandler(old_handler)
        sio = StringIO()
        siohandler = logging.StreamHandler(sio)
        logger.addHandler(siohandler)
        logger.log(loglevel, logmsg)
        logger.removeHandler(siohandler)
        # Reset original handlers
        logger.addHandler(old_handler)
        return sio.getvalue().strip()

    def assertDidLogTrue(self, logger, logmsg, reason, loglevel=None):
        val = self._assertLog(logger, logmsg, loglevel=loglevel)
        return self.assertEqual(val, logmsg, reason)

    def assertDidLogFalse(self, logger, logmsg, reason, loglevel=None):
        val = self._assertLog(logger, logmsg, loglevel=loglevel)
        return self.assertFalse(val, reason)

    def test_setup_logger(self):
        logger = setup_logger(loglevel=logging.ERROR, logfile=None)
        self.assertTrue(logger.handlers[0].stream is sys.stderr,
                "setup_logger logs to stderr without logfile argument.")
        #self.assertTrue(logger._process_aware,
        #        "setup_logger() returns process aware logger.")
        self.assertDidLogTrue(logger, "Logging something",
                "Logger logs error when loglevel is ERROR",
                loglevel=logging.ERROR)
        self.assertDidLogFalse(logger, "Logging something",
                "Logger doesn't info when loglevel is ERROR",
                loglevel=logging.INFO)

    def test_emergency_error(self):
        sio = StringIO()
        emergency_error(sio, "Testing emergency error facility")
        self.assertEquals(sio.getvalue().rpartition(":")[2].strip(),
                             "Testing emergency error facility")
