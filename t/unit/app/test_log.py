from __future__ import absolute_import, unicode_literals

import logging
import sys
from collections import defaultdict
from io import StringIO
from tempfile import mktemp

import pytest
from case import Mock, mock, patch, skip
from case.utils import get_logger_handlers

from celery import signals, uuid
from celery.app.log import TaskFormatter
from celery.five import python_2_unicode_compatible
from celery.utils.log import (ColorFormatter, LoggingProxy, get_logger,
                              get_task_logger, in_sighandler)
from celery.utils.log import logger as base_logger
from celery.utils.log import logger_isa, task_logger


class test_TaskFormatter:

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
        assert record.task_name == '???'
        assert record.task_id == '???'


class test_logger_isa:

    def test_isa(self):
        x = get_task_logger('Z1george')
        assert logger_isa(x, task_logger)
        prev_x, x.parent = x.parent, None
        try:
            assert not logger_isa(x, task_logger)
        finally:
            x.parent = prev_x

        y = get_task_logger('Z1elaine')
        y.parent = x
        assert logger_isa(y, task_logger)
        assert logger_isa(y, x)
        assert logger_isa(y, y)

        z = get_task_logger('Z1jerry')
        z.parent = y
        assert logger_isa(z, task_logger)
        assert logger_isa(z, y)
        assert logger_isa(z, x)
        assert logger_isa(z, z)

    def test_recursive(self):
        x = get_task_logger('X1foo')
        prev, x.parent = x.parent, x
        try:
            with pytest.raises(RuntimeError):
                logger_isa(x, task_logger)
        finally:
            x.parent = prev

        y = get_task_logger('X2foo')
        z = get_task_logger('X2foo')
        prev_y, y.parent = y.parent, z
        try:
            prev_z, z.parent = z.parent, y
            try:
                with pytest.raises(RuntimeError):
                    logger_isa(y, task_logger)
            finally:
                z.parent = prev_z
        finally:
            y.parent = prev_y


class test_ColorFormatter:

    @patch('celery.utils.log.safe_str')
    @patch('logging.Formatter.formatException')
    def test_formatException_not_string(self, fe, safe_str):
        x = ColorFormatter()
        value = KeyError()
        fe.return_value = value
        assert x.formatException(value) is value
        fe.assert_called()
        safe_str.assert_not_called()

    @patch('logging.Formatter.formatException')
    @patch('celery.utils.log.safe_str')
    def test_formatException_bytes(self, safe_str, fe):
        x = ColorFormatter()
        fe.return_value = b'HELLO'
        try:
            raise Exception()
        except Exception:
            assert x.formatException(sys.exc_info())
        if sys.version_info[0] == 2:
            safe_str.assert_called()

    @patch('logging.Formatter.format')
    def test_format_object(self, _format):
        x = ColorFormatter()
        x.use_color = True
        record = Mock()
        record.levelname = 'ERROR'
        record.msg = object()
        assert x.format(record)

    @patch('celery.utils.log.safe_str')
    def test_format_raises(self, safe_str):
        x = ColorFormatter()

        def on_safe_str(s):
            try:
                raise ValueError('foo')
            finally:
                safe_str.side_effect = None
        safe_str.side_effect = on_safe_str

        @python_2_unicode_compatible
        class Record(object):
            levelname = 'ERROR'
            msg = 'HELLO'
            exc_info = 1
            exc_text = 'error text'
            stack_info = None

            def __str__(self):
                return on_safe_str('')

            def getMessage(self):
                return self.msg

        record = Record()
        safe_str.return_value = record

        msg = x.format(record)
        assert '<Unrepresentable' in msg
        assert safe_str.call_count == 1

    @skip.if_python3()
    @patch('celery.utils.log.safe_str')
    def test_format_raises_no_color(self, safe_str):
        x = ColorFormatter(use_color=False)
        record = Mock()
        record.levelname = 'ERROR'
        record.msg = 'HELLO'
        record.exc_text = 'error text'
        x.format(record)
        assert safe_str.call_count == 1


class test_default_logger:

    def setup(self):
        self.setup_logger = self.app.log.setup_logger
        self.get_logger = lambda n=None: get_logger(n) if n else logging.root
        signals.setup_logging.receivers[:] = []
        self.app.log.already_setup = False

    def test_get_logger_sets_parent(self):
        logger = get_logger('celery.test_get_logger')
        assert logger.parent.name == base_logger.name

    def test_get_logger_root(self):
        logger = get_logger(base_logger.name)
        assert logger.parent is logging.root

    @mock.restore_logging()
    def test_setup_logging_subsystem_misc(self):
        self.app.log.setup_logging_subsystem(loglevel=None)

    @mock.restore_logging()
    def test_setup_logging_subsystem_misc2(self):
        self.app.conf.worker_hijack_root_logger = True
        self.app.log.setup_logging_subsystem()

    def test_get_default_logger(self):
        assert self.app.log.get_default_logger()

    def test_configure_logger(self):
        logger = self.app.log.get_default_logger()
        self.app.log._configure_logger(logger, sys.stderr, None, '', False)
        self.app.log._configure_logger(None, sys.stderr, None, '', False)
        logger.handlers[:] = []

    @mock.restore_logging()
    def test_setup_logging_subsystem_colorize(self):
        self.app.log.setup_logging_subsystem(colorize=None)
        self.app.log.setup_logging_subsystem(colorize=True)

    @mock.restore_logging()
    def test_setup_logging_subsystem_no_mputil(self):
        with mock.mask_modules('billiard.util'):
            self.app.log.setup_logging_subsystem()

    @mock.restore_logging()
    def test_setup_logger(self):
        logger = self.setup_logger(loglevel=logging.ERROR, logfile=None,
                                   root=False, colorize=True)
        logger.handlers = []
        self.app.log.already_setup = False
        logger = self.setup_logger(loglevel=logging.ERROR, logfile=None,
                                   root=False, colorize=None)
        # setup_logger logs to stderr without logfile argument.
        assert (get_logger_handlers(logger)[0].stream is
                sys.__stderr__)

    @mock.restore_logging()
    def test_setup_logger_no_handlers_stream(self):
        l = self.get_logger()
        l.handlers = []

        with mock.stdouts() as outs:
            stdout, stderr = outs
            l = self.setup_logger(logfile=sys.stderr,
                                  loglevel=logging.INFO, root=False)
            l.info('The quick brown fox...')
            assert 'The quick brown fox...' in stderr.getvalue()

    @patch('os.fstat')
    def test_setup_logger_no_handlers_file(self, *args):
        tempfile = mktemp(suffix='unittest', prefix='celery')
        _open = ('builtins.open' if sys.version_info[0] == 3
                 else '__builtin__.open')
        with patch(_open) as osopen:
            with mock.restore_logging():
                files = defaultdict(StringIO)

                def open_file(filename, *args, **kwargs):
                    f = files[filename]
                    f.fileno = Mock()
                    f.fileno.return_value = 99
                    return f

                osopen.side_effect = open_file
                l = self.get_logger()
                l.handlers = []
                l = self.setup_logger(
                    logfile=tempfile, loglevel=logging.INFO, root=False,
                )
                assert isinstance(get_logger_handlers(l)[0],
                                  logging.FileHandler)
                assert tempfile in files

    @mock.restore_logging()
    def test_redirect_stdouts(self):
        logger = self.setup_logger(loglevel=logging.ERROR, logfile=None,
                                   root=False)
        try:
            with mock.wrap_logger(logger) as sio:
                self.app.log.redirect_stdouts_to_logger(
                    logger, loglevel=logging.ERROR,
                )
                logger.error('foo')
                assert 'foo' in sio.getvalue()
                self.app.log.redirect_stdouts_to_logger(
                    logger, stdout=False, stderr=False,
                )
        finally:
            sys.stdout, sys.stderr = sys.__stdout__, sys.__stderr__

    @mock.restore_logging()
    def test_logging_proxy(self):
        logger = self.setup_logger(loglevel=logging.ERROR, logfile=None,
                                   root=False)

        with mock.wrap_logger(logger) as sio:
            p = LoggingProxy(logger, loglevel=logging.ERROR)
            p.close()
            p.write('foo')
            assert 'foo' not in sio.getvalue()
            p.closed = False
            p.write('foo')
            assert 'foo' in sio.getvalue()
            lines = ['baz', 'xuzzy']
            p.writelines(lines)
            for line in lines:
                assert line in sio.getvalue()
            p.flush()
            p.close()
            assert not p.isatty()

            with mock.stdouts() as (stdout, stderr):
                with in_sighandler():
                    p.write('foo')
                    assert stderr.getvalue()

    @mock.restore_logging()
    def test_logging_proxy_recurse_protection(self):
        logger = self.setup_logger(loglevel=logging.ERROR, logfile=None,
                                   root=False)
        p = LoggingProxy(logger, loglevel=logging.ERROR)
        p._thread.recurse_protection = True
        try:
            assert p.write('FOOFO') is None
        finally:
            p._thread.recurse_protection = False


class test_task_logger(test_default_logger):

    def setup(self):
        logger = self.logger = get_logger('celery.task')
        logger.handlers = []
        logging.root.manager.loggerDict.pop(logger.name, None)
        self.uid = uuid()

        @self.app.task(shared=False)
        def test_task():
            pass
        self.get_logger().handlers = []
        self.task = test_task
        from celery._state import _task_stack
        _task_stack.push(test_task)

    def teardown(self):
        from celery._state import _task_stack
        _task_stack.pop()

    def setup_logger(self, *args, **kwargs):
        return self.app.log.setup_task_loggers(*args, **kwargs)

    def get_logger(self, *args, **kwargs):
        return get_task_logger('test_task_logger')

    def test_renaming_base_logger(self):
        with pytest.raises(RuntimeError):
            get_task_logger('celery')

    def test_renaming_task_logger(self):
        with pytest.raises(RuntimeError):
            get_task_logger('celery.task')


class MockLogger(logging.Logger):
    _records = None

    def __init__(self, *args, **kwargs):
        self._records = []
        logging.Logger.__init__(self, *args, **kwargs)

    def handle(self, record):
        self._records.append(record)

    def isEnabledFor(self, level):
        return True
