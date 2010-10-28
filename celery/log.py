"""celery.log"""
import logging
import threading
import sys
import traceback
import types

from multiprocessing import current_process
from multiprocessing import util as mputil

from celery import signals
from celery.app import app_or_default
from celery.utils import LOG_LEVELS
from celery.utils.compat import LoggerAdapter
from celery.utils.patch import ensure_process_aware_logger
from celery.utils.term import colored

# The logging subsystem is only configured once per process.
# setup_logging_subsystem sets this flag, and subsequent calls
# will do nothing.
_setup = False

COLORS = {"DEBUG": "blue",
          "WARNING": "yellow",
          "ERROR": "red",
          "CRITICAL": "magenta"}


class ColorFormatter(logging.Formatter):

    def __init__(self, msg, use_color=True):
        logging.Formatter.__init__(self, msg)
        self.use_color = use_color

    def formatException(self, ei):
        r = logging.Formatter.formatException(self, ei)
        if type(r) in [types.StringType]:
            r = r.decode("utf-8", "replace") # Convert to unicode
        return r

    def format(self, record):
        levelname = record.levelname

        if self.use_color and levelname in COLORS:
            record.msg = unicode(colored().names[COLORS[levelname]](record.msg))

        # Very ugly, but have to make sure processName is supported
        # by foreign logger instances.
        # (processName is always supported by Python 2.7)
        if "processName" not in record.__dict__:
            record.__dict__["processName"] = current_process()._name
        t = logging.Formatter.format(self, record)
        if type(t) in [types.UnicodeType]:
            t = t.encode('utf-8', 'replace')
        return t


class Logging(object):
    _setup = False

    def __init__(self, app):
        self.app = app
        self.loglevel = self.app.conf.CELERYD_LOG_LEVEL
        self.format = self.app.conf.CELERYD_LOG_FORMAT

    def get_task_logger(self, loglevel=None, name=None):
        logger = logging.getLogger(name or "celery.task.default")
        if loglevel is not None:
            logger.setLevel(loglevel)
        return logger

    def setup_logging_subsystem(self, loglevel=None, logfile=None,
            format=None, colorize=None, **kwargs):
        loglevel = loglevel or self.loglevel
        format = format or self.format
        colorize = self.app.either("CELERYD_LOG_COLOR", colorize)

        if self.__class__._setup:
            return

        try:
            mputil._logger = None
        except AttributeError:
            pass
        ensure_process_aware_logger()
        logging.Logger.manager.loggerDict.clear()
        receivers = signals.setup_logging.send(sender=None,
                                               loglevel=loglevel,
                                               logfile=logfile,
                                               format=format,
                                               colorize=colorize)
        if not receivers:
            root = logging.getLogger()
            mp = mputil.get_logger()
            for logger in (root, mp):
                self._setup_logger(logger, logfile,
                                   format, colorize, **kwargs)
                logger.setLevel(loglevel)
        self.__class__._setup = True
        return receivers

    def _detect_handler(self, logfile=None):
        """Create log handler with either a filename, an open stream
        or :const:`None` (stderr)."""
        if not logfile or hasattr(logfile, "write"):
            return logging.StreamHandler(logfile)
        return logging.FileHandler(logfile)

    def get_default_logger(self, loglevel=None, name="celery"):
        """Get default logger instance.

        :keyword loglevel: Initial log level.

        """
        logger = logging.getLogger(name)
        if loglevel is not None:
            logger.setLevel(loglevel)
        return logger

    def setup_logger(self, loglevel=None, logfile=None,
            format=None, colorize=None, name="celery", root=True,
            app=None, **kwargs):
        """Setup the :mod:`multiprocessing` logger.

        If `logfile` is not specified, then `sys.stderr` is used.

        Returns logger object.

        """
        loglevel = loglevel or self.loglevel
        format = format or self.format
        colorize = self.app.either("CELERYD_LOG_COLOR", colorize)

        if not root:
            return self._setup_logger(self.get_default_logger(loglevel, name),
                                      logfile, format, colorize, **kwargs)
        self.setup_logging_subsystem(loglevel, logfile,
                                     format, colorize, **kwargs)
        return self.get_default_logger(name=name)

    def setup_task_logger(self, loglevel=None, logfile=None, format=None,
            colorize=None, task_kwargs=None, app=None, **kwargs):
        """Setup the task logger.

        If `logfile` is not specified, then `sys.stderr` is used.

        Returns logger object.

        """
        loglevel = loglevel or self.loglevel
        format = format or self.format
        colorize = self.app.either("CELERYD_LOG_COLOR", colorize)

        if task_kwargs is None:
            task_kwargs = {}
        task_kwargs.setdefault("task_id", "-?-")
        task_name = task_kwargs.get("task_name")
        task_kwargs.setdefault("task_name", "-?-")
        logger = self._setup_logger(self.get_task_logger(loglevel, task_name),
                                    logfile, format, colorize, **kwargs)
        return LoggerAdapter(logger, task_kwargs)

    def redirect_stdouts_to_logger(self, logger, loglevel=None):
        """Redirect :class:`sys.stdout` and :class:`sys.stderr` to a
        logging instance.

        :param logger: The :class:`logging.Logger` instance to redirect to.
        :param loglevel: The loglevel redirected messages will be logged as.

        """
        proxy = LoggingProxy(logger, loglevel)
        sys.stdout = sys.stderr = proxy
        return proxy

    def _setup_logger(self, logger, logfile, format, colorize,
            formatter=ColorFormatter, **kwargs):

        if logger.handlers:                 # Logger already configured
            return logger

        handler = self._detect_handler(logfile)
        handler.setFormatter(formatter(format, use_color=colorize))
        logger.addHandler(handler)
        return logger


_default_logging = Logging(app_or_default())
setup_logging_subsystem = _default_logging.setup_logging_subsystem
get_default_logger = _default_logging.get_default_logger
setup_logger = _default_logging.setup_logger
setup_task_logger = _default_logging.setup_task_logger
get_task_logger = _default_logging.get_task_logger
redirect_stdouts_to_logger = _default_logging.redirect_stdouts_to_logger


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
        self.loglevel = loglevel or self.logger.level or self.loglevel
        if not isinstance(self.loglevel, int):
            self.loglevel = LOG_LEVELS[self.loglevel.upper()]
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
                self.logger.log(self.loglevel, data)
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
        return None


class SilenceRepeated(object):
    """Only log action every n iterations."""

    def __init__(self, action, max_iterations=10):
        self.action = action
        self.max_iterations = max_iterations
        self._iterations = 0

    def __call__(self, *msgs):
        if self._iterations >= self.max_iterations:
            for msg in msgs:
                self.action(msg)
            self._iterations = 0
        else:
            self._iterations += 1
