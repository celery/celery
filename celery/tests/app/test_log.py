from __future__ import absolute_import
from __future__ import with_statement

import sys
import logging
from tempfile import mktemp

from mock import patch, Mock
from nose import SkipTest

from celery import current_app
from celery import signals
from celery.app.log import Logging, TaskFormatter
from celery.utils.log import LoggingProxy
from celery.utils import uuid
from celery.utils.log import (
    get_logger,
    ColorFormatter,
    logger as base_logger,
    get_task_logger,
)
from celery.tests.utils import (
    AppCase, Case, override_stdouts, wrap_logger, get_handlers,
)

log = current_app.log


class test_TaskFormatter(Case):

    def test_no_task(self):
        class Record(object):
            msg = 'hello world'
            levelname = 'info'
            exc_text = exc_info = None
            stack_info = None

            def getMessage(self):
                return self.msg
        record = Record()
        x = TaskFormatter()
        x.format(record)
        self.assertEqual(record.task_name, '???')
        self.assertEqual(record.task_id, '???')


class test_ColorFormatter(Case):

    @patch('celery.utils.log.safe_str')
    @patch('logging.Formatter.formatException')
    def test_formatException_not_string(self, fe, safe_str):
        x = ColorFormatter('HELLO')
        value = KeyError()
        fe.return_value = value
        self.assertIs(x.formatException(value), value)
        self.assertTrue(fe.called)
        self.assertFalse(safe_str.called)

    @patch('logging.Formatter.formatException')
    @patch('celery.utils.log.safe_str')
    def test_formatException_string(self, safe_str, fe, value='HELLO'):
        x = ColorFormatter(value)
        fe.return_value = value
        self.assertTrue(x.formatException(value))
        if sys.version_info[0] == 2:
            self.assertTrue(safe_str.called)

    @patch('celery.utils.log.safe_str')
    def test_format_raises(self, safe_str):
        x = ColorFormatter('HELLO')

        def on_safe_str(s):
            try:
                raise ValueError('foo')
            finally:
                safe_str.side_effect = None
        safe_str.side_effect = on_safe_str

        class Record(object):
            levelname = 'ERROR'
            msg = 'HELLO'
            exc_text = 'error text'
            stack_info = None

            def __str__(self):
                return on_safe_str('')

            def getMessage(self):
                return self.msg

        record = Record()
        safe_str.return_value = record

        x.format(record)
        self.assertIn('<Unrepresentable', record.msg)
        self.assertEqual(safe_str.call_count, 2)

    @patch('celery.utils.log.safe_str')
    def test_format_raises_no_color(self, safe_str):
        if sys.version_info[0] == 3:
            raise SkipTest('py3k')
        x = ColorFormatter('HELLO', False)
        record = Mock()
        record.levelname = 'ERROR'
        record.msg = 'HELLO'
        record.exc_text = 'error text'
        x.format(record)
        self.assertEqual(safe_str.call_count, 1)


class test_default_logger(AppCase):

    def setup(self):
        self.setup_logger = log.setup_logger
        self.get_logger = lambda n=None: get_logger(n) if n else logging.root
        signals.setup_logging.receivers[:] = []
        Logging._setup = False

    def test_get_logger_sets_parent(self):
        logger = get_logger('celery.test_get_logger')
        self.assertEqual(logger.parent.name, base_logger.name)

    def test_get_logger_root(self):
        logger = get_logger(base_logger.name)
        self.assertIs(logger.parent, logging.root)

    def test_setup_logging_subsystem_misc(self):
        log.setup_logging_subsystem(loglevel=None)
        self.app.conf.CELERYD_HIJACK_ROOT_LOGGER = True
        try:
            log.setup_logging_subsystem()
        finally:
            self.app.conf.CELERYD_HIJACK_ROOT_LOGGER = False

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
        self.assertIs(
            get_handlers(logger)[0].stream, sys.__stderr__,
            'setup_logger logs to stderr without logfile argument.',
        )

    def test_setup_logger_no_handlers_stream(self):
        l = self.get_logger()
        l.handlers = []

        with override_stdouts() as outs:
            stdout, stderr = outs
            l = self.setup_logger(logfile=sys.stderr, loglevel=logging.INFO,
                                  root=False)
            l.info('The quick brown fox...')
            self.assertIn('The quick brown fox...', stderr.getvalue())

    def test_setup_logger_no_handlers_file(self):
        l = self.get_logger()
        l.handlers = []
        tempfile = mktemp(suffix='unittest', prefix='celery')
        l = self.setup_logger(logfile=tempfile, loglevel=0, root=False)
        self.assertIsInstance(get_handlers(l)[0],
                              logging.FileHandler)

    def test_redirect_stdouts(self):
        logger = self.setup_logger(loglevel=logging.ERROR, logfile=None,
                                   root=False)
        try:
            with wrap_logger(logger) as sio:
                log.redirect_stdouts_to_logger(logger, loglevel=logging.ERROR)
                logger.error('foo')
                self.assertIn('foo', sio.getvalue())
                log.redirect_stdouts_to_logger(logger, stdout=False,
                                               stderr=False)
        finally:
            sys.stdout, sys.stderr = sys.__stdout__, sys.__stderr__

    def test_logging_proxy(self):
        logger = self.setup_logger(loglevel=logging.ERROR, logfile=None,
                                   root=False)

        with wrap_logger(logger) as sio:
            p = LoggingProxy(logger, loglevel=logging.ERROR)
            p.close()
            p.write('foo')
            self.assertNotIn('foo', sio.getvalue())
            p.closed = False
            p.write('foo')
            self.assertIn('foo', sio.getvalue())
            lines = ['baz', 'xuzzy']
            p.writelines(lines)
            for line in lines:
                self.assertIn(line, sio.getvalue())
            p.flush()
            p.close()
            self.assertFalse(p.isatty())

    def test_logging_proxy_recurse_protection(self):
        logger = self.setup_logger(loglevel=logging.ERROR, logfile=None,
                                   root=False)
        p = LoggingProxy(logger, loglevel=logging.ERROR)
        p._thread.recurse_protection = True
        try:
            self.assertIsNone(p.write('FOOFO'))
        finally:
            p._thread.recurse_protection = False


class test_task_logger(test_default_logger):

    def setup(self):
        logger = self.logger = get_logger('celery.task')
        logger.handlers = []
        logging.root.manager.loggerDict.pop(logger.name, None)
        self.uid = uuid()

        @current_app.task
        def test_task():
            pass
        self.get_logger().handlers = []
        self.task = test_task
        from celery._state import _task_stack
        _task_stack.push(test_task)

    def tearDown(self):
        from celery._state import _task_stack
        _task_stack.pop()

    def setup_logger(self, *args, **kwargs):
        return log.setup_task_loggers(*args, **kwargs)

    def get_logger(self, *args, **kwargs):
        return get_task_logger("test_task_logger")


class MockLogger(logging.Logger):
    _records = None

    def __init__(self, *args, **kwargs):
        self._records = []
        logging.Logger.__init__(self, *args, **kwargs)

    def handle(self, record):
        self._records.append(record)

    def isEnabledFor(self, level):
        return True
