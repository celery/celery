# -*- coding: utf-8 -*-
"""
    celery.utils.log
    ~~~~~~~~~~~~~~~~

    Logging utilities.

"""
from __future__ import absolute_import

import logging
import os
import sys
import threading
import traceback

from billiard import current_process, util as mputil
from kombu.log import get_logger as _get_logger, LOG_LEVELS

from .encoding import safe_str, str_t
from .term import colored

_process_aware = False
is_py3k = sys.version_info[0] == 3

MP_LOG = os.environ.get('MP_LOG', False)


# Sets up our logging hierarchy.
#
# Every logger in the celery package inherits from the "celery"
# logger, and every task logger inherits from the "celery.task"
# logger.
base_logger = logger = _get_logger('celery')
mp_logger = _get_logger('multiprocessing')

in_sighandler = False


def set_in_sighandler(value):
    global in_sighandler
    in_sighandler = value


def get_logger(name):
    l = _get_logger(name)
    if logging.root not in (l, l.parent) and l is not base_logger:
        l.parent = base_logger
    return l
task_logger = get_logger('celery.task')


def get_task_logger(name):
    logger = get_logger(name)
    if logger.parent is logging.root:
        logger.parent = task_logger
    return logger


def mlevel(level):
    if level and not isinstance(level, int):
        return LOG_LEVELS[level.upper()]
    return level


class ColorFormatter(logging.Formatter):
    #: Loglevel -> Color mapping.
    COLORS = colored().names
    colors = {'DEBUG': COLORS['blue'], 'WARNING': COLORS['yellow'],
              'ERROR': COLORS['red'], 'CRITICAL': COLORS['magenta']}

    def __init__(self, fmt=None, use_color=True):
        logging.Formatter.__init__(self, fmt)
        self.use_color = use_color

    def formatException(self, ei):
        if ei and not isinstance(ei, tuple):
            ei = sys.exc_info()
        r = logging.Formatter.formatException(self, ei)
        if isinstance(r, str) and not is_py3k:
            return safe_str(r)
        return r

    def format(self, record):
        levelname = record.levelname
        color = self.colors.get(levelname)

        if self.use_color and color:
            try:
                msg = record.msg
                # safe_str will repr the color object
                # and color will break on non-string objects
                # so need to reorder calls based on type.
                # Issue #427
                if isinstance(msg, basestring):
                    record.msg = str_t(color(safe_str(msg)))
                else:
                    record.msg = safe_str(color(msg))
            except Exception, exc:
                record.msg = '<Unrepresentable %r: %r>' % (
                    type(record.msg), exc)
                record.exc_info = True

        if not is_py3k and 'processName' not in record.__dict__:
            # Very ugly, but have to make sure processName is supported
            # by foreign logger instances.
            # (processName is always supported by Python 2.7)
            process_name = current_process and current_process()._name or ''
            record.__dict__['processName'] = process_name
        return safe_str(logging.Formatter.format(self, record))


class LoggingProxy(object):
    """Forward file object to :class:`logging.Logger` instance.

    :param logger: The :class:`logging.Logger` instance to forward to.
    :param loglevel: Loglevel to use when writing messages.

    """
    mode = 'w'
    name = None
    closed = False
    loglevel = logging.ERROR
    _thread = threading.local()

    def __init__(self, logger, loglevel=None):
        self.logger = logger
        self.loglevel = mlevel(loglevel or self.logger.level or self.loglevel)
        self._safewrap_handlers()

    def _safewrap_handlers(self):
        """Make the logger handlers dump internal errors to
        `sys.__stderr__` instead of `sys.stderr` to circumvent
        infinite loops."""

        def wrap_handler(handler):                  # pragma: no cover

            class WithSafeHandleError(logging.Handler):

                def handleError(self, record):
                    exc_info = sys.exc_info()
                    try:
                        try:
                            traceback.print_exception(exc_info[0],
                                                      exc_info[1],
                                                      exc_info[2],
                                                      None, sys.__stderr__)
                        except IOError:
                            pass    # see python issue 5971
                    finally:
                        del(exc_info)

            handler.handleError = WithSafeHandleError().handleError

        return [wrap_handler(h) for h in self.logger.handlers]

    def write(self, data):
        """Write message to logging object."""
        if in_sighandler:
            return sys.__stderr__.write(safe_str(data))
        if getattr(self._thread, 'recurse_protection', False):
            # Logger is logging back to this file, so stop recursing.
            return
        data = data.strip()
        if data and not self.closed:
            self._thread.recurse_protection = True
            try:
                self.logger.log(self.loglevel, safe_str(data))
            finally:
                self._thread.recurse_protection = False

    def writelines(self, sequence):
        """`writelines(sequence_of_strings) -> None`.

        Write the strings to the file.

        The sequence can be any iterable object producing strings.
        This is equivalent to calling :meth:`write` for each string.

        """
        for part in sequence:
            self.write(part)

    def flush(self):
        """This object is not buffered so any :meth:`flush` requests
        are ignored."""
        pass

    def close(self):
        """When the object is closed, no write requests are forwarded to
        the logging object anymore."""
        self.closed = True

    def isatty(self):
        """Always returns :const:`False`. Just here for file support."""
        return False


def ensure_process_aware_logger():
    """Make sure process name is recorded when loggers are used."""
    global _process_aware
    if not _process_aware:
        logging._acquireLock()
        try:
            _process_aware = True
            Logger = logging.getLoggerClass()
            if getattr(Logger, '_process_aware', False):  # pragma: no cover
                return

            class ProcessAwareLogger(Logger):
                _process_aware = True

                def makeRecord(self, *args, **kwds):
                    record = Logger.makeRecord(self, *args, **kwds)
                    record.processName = current_process()._name
                    return record
            logging.setLoggerClass(ProcessAwareLogger)
        finally:
            logging._releaseLock()


def get_multiprocessing_logger():
    return mputil.get_logger() if mputil else None


def reset_multiprocessing_logger():
    if mputil and hasattr(mputil, '_logger'):
        mputil._logger = None


def _patch_logger_class():
    """Make sure loggers don't log while in a signal handler."""

    logging._acquireLock()
    try:
        OldLoggerClass = logging.getLoggerClass()
        if not getattr(OldLoggerClass, '_signal_safe', False):

            class SigSafeLogger(OldLoggerClass):
                _signal_safe = True

                def log(self, *args, **kwargs):
                    if in_sighandler:
                        sys.__stderr__.write('CANNOT LOG IN SIGHANDLER')
                        return
                    return OldLoggerClass.log(self, *args, **kwargs)
            logging.setLoggerClass(SigSafeLogger)
    finally:
        logging._releaseLock()
_patch_logger_class()
