# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging
import threading
import os
import sys
import traceback

try:
    from multiprocessing import current_process
    from multiprocessing import util as mputil
except ImportError:
    current_process = mputil = None  # noqa

from . import current_app
from . import signals
from .local import Proxy
from .utils import LOG_LEVELS, isatty
from .utils.compat import LoggerAdapter, WatchedFileHandler
from .utils.encoding import safe_str, str_t
from .utils.patch import ensure_process_aware_logger
from .utils.term import colored

is_py3k = sys.version_info >= (3, 0)


def mlevel(level):
    if level and not isinstance(level, int):
        return LOG_LEVELS[level.upper()]
    return level


class ColorFormatter(logging.Formatter):
    #: Loglevel -> Color mapping.
    COLORS = colored().names
    colors = {"DEBUG": COLORS["blue"], "WARNING": COLORS["yellow"],
              "ERROR": COLORS["red"], "CRITICAL": COLORS["magenta"]}

    def __init__(self, msg, use_color=True):
        logging.Formatter.__init__(self, msg)
        self.use_color = use_color

    def formatException(self, ei):
        r = logging.Formatter.formatException(self, ei)
        if isinstance(r, str) and not is_py3k:
            return safe_str(r)
        return r

    def format(self, record):
        levelname = record.levelname
        color = self.colors.get(levelname)

        if self.use_color and color:
            try:
                record.msg = safe_str(str_t(color(record.msg)))
            except Exception, exc:
                record.msg = "<Unrepresentable %r: %r>" % (
                        type(record.msg), exc)
                record.exc_info = sys.exc_info()

        if not is_py3k:
            # Very ugly, but have to make sure processName is supported
            # by foreign logger instances.
            # (processName is always supported by Python 2.7)
            if "processName" not in record.__dict__:
                process_name = (current_process and
                                current_process()._name or "")
                record.__dict__["processName"] = process_name
        return safe_str(logging.Formatter.format(self, record))


class Logging(object):
    #: The logging subsystem is only configured once per process.
    #: setup_logging_subsystem sets this flag, and subsequent calls
    #: will do nothing.
    _setup = False

    def __init__(self, app):
        self.app = app
        self.loglevel = mlevel(self.app.conf.CELERYD_LOG_LEVEL)
        self.format = self.app.conf.CELERYD_LOG_FORMAT
        self.task_format = self.app.conf.CELERYD_TASK_LOG_FORMAT
        self.colorize = self.app.conf.CELERYD_LOG_COLOR

    def supports_color(self, logfile=None):
        if self.app.IS_WINDOWS:
            # Windows does not support ANSI color codes.
            return False
        if self.colorize is None:
            # Only use color if there is no active log file
            # and stderr is an actual terminal.
            return logfile is None and isatty(sys.stderr)
        return self.colorize

    def colored(self, logfile=None):
        return colored(enabled=self.supports_color(logfile))

    def get_task_logger(self, loglevel=None, name=None):
        logger = logging.getLogger(name or "celery.task.default")
        if loglevel is not None:
            logger.setLevel(mlevel(loglevel))
        return logger

    def setup_logging_subsystem(self, loglevel=None, logfile=None,
            format=None, colorize=None, **kwargs):
        if Logging._setup:
            return
        loglevel = mlevel(loglevel or self.loglevel)
        format = format or self.format
        if colorize is None:
            colorize = self.supports_color(logfile)

        if mputil and hasattr(mputil, "_logger"):
            mputil._logger = None
        if not is_py3k:
            ensure_process_aware_logger()
        receivers = signals.setup_logging.send(sender=None,
                        loglevel=loglevel, logfile=logfile,
                        format=format, colorize=colorize)
        if not receivers:
            root = logging.getLogger()

            if self.app.conf.CELERYD_HIJACK_ROOT_LOGGER:
                root.handlers = []

            mp = mputil.get_logger() if mputil else None
            for logger in filter(None, (root, mp)):
                self._setup_logger(logger, logfile, format, colorize, **kwargs)
                logger.setLevel(mlevel(loglevel))
                signals.after_setup_logger.send(sender=None, logger=logger,
                                        loglevel=loglevel, logfile=logfile,
                                        format=format, colorize=colorize)

        # This is a hack for multiprocessing's fork+exec, so that
        # logging before Process.run works.
        os.environ.update(_MP_FORK_LOGLEVEL_=str(loglevel),
                          _MP_FORK_LOGFILE_=logfile or "",
                          _MP_FORK_LOGFORMAT_=format)
        Logging._setup = True

        return receivers

    def setup(self, loglevel=None, logfile=None, redirect_stdouts=False,
            redirect_level="WARNING"):
        handled = self.setup_logging_subsystem(loglevel=loglevel,
                                               logfile=logfile)
        if not handled:
            logger = self.get_default_logger()
            if redirect_stdouts:
                self.redirect_stdouts_to_logger(logger,
                                loglevel=redirect_level)
        os.environ.update(
            CELERY_LOG_LEVEL=str(loglevel) if loglevel else "",
            CELERY_LOG_FILE=str(logfile) if logfile else "",
            CELERY_LOG_REDIRECT="1" if redirect_stdouts else "",
            CELERY_LOG_REDIRECT_LEVEL=str(redirect_level))

    def _detect_handler(self, logfile=None):
        """Create log handler with either a filename, an open stream
        or :const:`None` (stderr)."""
        logfile = sys.__stderr__ if logfile is None else logfile
        if hasattr(logfile, "write"):
            return logging.StreamHandler(logfile)
        return WatchedFileHandler(logfile)

    def get_default_logger(self, loglevel=None, name="celery"):
        """Get default logger instance.

        :keyword loglevel: Initial log level.

        """
        logger = logging.getLogger(name)
        if loglevel is not None:
            logger.setLevel(mlevel(loglevel))
        return logger

    def setup_logger(self, loglevel=None, logfile=None,
            format=None, colorize=None, name="celery", root=True,
            app=None, **kwargs):
        """Setup the :mod:`multiprocessing` logger.

        If `logfile` is not specified, then `sys.stderr` is used.

        Returns logger object.

        """
        loglevel = mlevel(loglevel or self.loglevel)
        format = format or self.format
        if colorize is None:
            colorize = self.supports_color(logfile)

        if not root or self.app.conf.CELERYD_HIJACK_ROOT_LOGGER:
            return self._setup_logger(self.get_default_logger(loglevel, name),
                                      logfile, format, colorize, **kwargs)
        self.setup_logging_subsystem(loglevel, logfile,
                                     format, colorize, **kwargs)
        return self.get_default_logger(name=name)

    def setup_task_logger(self, loglevel=None, logfile=None, format=None,
            colorize=None, task_name=None, task_id=None, propagate=False,
            app=None, **kwargs):
        """Setup the task logger.

        If `logfile` is not specified, then `sys.stderr` is used.

        Returns logger object.

        """
        loglevel = mlevel(loglevel or self.loglevel)
        format = format or self.task_format
        if colorize is None:
            colorize = self.supports_color(logfile)

        logger = self._setup_logger(self.get_task_logger(loglevel, task_name),
                                    logfile, format, colorize, **kwargs)
        logger.propagate = int(propagate)    # this is an int for some reason.
                                             # better to not question why.
        signals.after_setup_task_logger.send(sender=None, logger=logger,
                                     loglevel=loglevel, logfile=logfile,
                                     format=format, colorize=colorize)
        return LoggerAdapter(logger, {"task_id": task_id,
                                      "task_name": task_name})

    def redirect_stdouts_to_logger(self, logger, loglevel=None,
            stdout=True, stderr=True):
        """Redirect :class:`sys.stdout` and :class:`sys.stderr` to a
        logging instance.

        :param logger: The :class:`logging.Logger` instance to redirect to.
        :param loglevel: The loglevel redirected messages will be logged as.

        """
        proxy = LoggingProxy(logger, loglevel)
        if stdout:
            sys.stdout = proxy
        if stderr:
            sys.stderr = proxy
        return proxy

    def _is_configured(self, logger):
        return logger.handlers and not getattr(
                logger, "_rudimentary_setup", False)

    def _setup_logger(self, logger, logfile, format, colorize,
            formatter=ColorFormatter, **kwargs):
        if self._is_configured(logger):
            return logger

        handler = self._detect_handler(logfile)
        handler.setFormatter(formatter(format, use_color=colorize))
        logger.addHandler(handler)
        return logger


get_default_logger = Proxy(lambda: current_app.log.get_default_logger)
setup_logger = Proxy(lambda: current_app.log.setup_logger)
setup_task_logger = Proxy(lambda: current_app.log.setup_task_logger)
get_task_logger = Proxy(lambda: current_app.log.get_task_logger)
setup_logging_subsystem = Proxy(
            lambda: current_app.log.setup_logging_subsystem)
redirect_stdouts_to_logger = Proxy(
            lambda: current_app.log.redirect_stdouts_to_logger)


class LoggingProxy(object):
    """Forward file object to :class:`logging.Logger` instance.

    :param logger: The :class:`logging.Logger` instance to forward to.
    :param loglevel: Loglevel to use when writing messages.

    """
    mode = "w"
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

        return map(wrap_handler, self.logger.handlers)

    def write(self, data):
        if getattr(self._thread, "recurse_protection", False):
            # Logger is logging back to this file, so stop recursing.
            return
        """Write message to logging object."""
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

    def fileno(self):
        pass


class SilenceRepeated(object):
    """Only log action every n iterations."""

    def __init__(self, action, max_iterations=10):
        self.action = action
        self.max_iterations = max_iterations
        self._iterations = 0

    def __call__(self, *args, **kwargs):
        if not self._iterations or self._iterations >= self.max_iterations:
            self.action(*args, **kwargs)
            self._iterations = 0
        else:
            self._iterations += 1
