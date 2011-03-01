"""celery.log"""
import logging
import threading
import sys
import traceback

try:
    from multiprocessing import current_process
    from multiprocessing import util as mputil
except ImportError:
    current_process = mputil = None

from celery import signals
from celery import current_app
from celery.utils import LOG_LEVELS, isatty
from celery.utils.compat import LoggerAdapter
from celery.utils.encoding import safe_str
from celery.utils.patch import ensure_process_aware_logger
from celery.utils.term import colored


class ColorFormatter(logging.Formatter):
    #: Loglevel -> Color mapping.
    COLORS = colored().names
    colors = {"DEBUG": COLORS["blue"], "WARNING": COLORS["yellow"],
              "ERROR": COLORS["red"],  "CRITICAL": COLORS["magenta"]}

    def __init__(self, msg, use_color=True):
        logging.Formatter.__init__(self, msg)
        self.use_color = use_color

    def formatException(self, ei):
        r = logging.Formatter.formatException(self, ei)
        if isinstance(r, str):
            return r.decode("utf-8", "replace")    # Convert to unicode
        return r

    def format(self, record):
        levelname = record.levelname
        color = self.colors.get(levelname)

        if self.use_color and color:
            try:
                record.msg = color(safe_str(record.msg))
            except Exception:
                record.msg = "<Unrepresentable %r: %r>" % (
                        type(record.msg), traceback.format_stack())

        # Very ugly, but have to make sure processName is supported
        # by foreign logger instances.
        # (processName is always supported by Python 2.7)
        if "processName" not in record.__dict__:
            process_name = current_process and current_process()._name or ""
            record.__dict__["processName"] = process_name
        t = logging.Formatter.format(self, record)
        if isinstance(t, unicode):
            return t.encode("utf-8", "replace")
        return t


class Logging(object):
    #: The logging subsystem is only configured once per process.
    #: setup_logging_subsystem sets this flag, and subsequent calls
    #: will do nothing.
    _setup = False

    def __init__(self, app):
        self.app = app
        self.loglevel = self.app.conf.CELERYD_LOG_LEVEL
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
            logger.setLevel(loglevel)
        return logger

    def setup_logging_subsystem(self, loglevel=None, logfile=None,
            format=None, colorize=None, **kwargs):
        if Logging._setup:
            return
        loglevel = loglevel or self.loglevel
        format = format or self.format
        if colorize is None:
            colorize = self.supports_color(logfile)

        if mputil:
            try:
                mputil._logger = None
            except AttributeError:
                pass
        ensure_process_aware_logger()
        receivers = signals.setup_logging.send(sender=None,
                                               loglevel=loglevel,
                                               logfile=logfile,
                                               format=format,
                                               colorize=colorize)
        if not receivers:
            root = logging.getLogger()

            if self.app.conf.CELERYD_HIJACK_ROOT_LOGGER:
                root.handlers = []

            mp = mputil and mputil.get_logger() or None
            for logger in (root, mp):
                if logger:
                    self._setup_logger(logger, logfile, format,
                                       colorize, **kwargs)
                    logger.setLevel(loglevel)
        Logging._setup = True
        return receivers

    def _detect_handler(self, logfile=None):
        """Create log handler with either a filename, an open stream
        or :const:`None` (stderr)."""
        if logfile is None:
            logfile = sys.__stderr__
        if hasattr(logfile, "write"):
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
        loglevel = loglevel or self.loglevel
        format = format or self.task_format
        if colorize is None:
            colorize = self.supports_color(logfile)

        logger = self._setup_logger(self.get_task_logger(loglevel, task_name),
                                    logfile, format, colorize, **kwargs)
        logger.propagate = int(propagate)    # this is an int for some reason.
                                             # better to not question why.
        return LoggerAdapter(logger, {"task_id": task_id,
                                      "task_name": task_name})

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

        if logger.handlers:  # Logger already configured
            return logger

        handler = self._detect_handler(logfile)
        handler.setFormatter(formatter(format, use_color=colorize))
        logger.addHandler(handler)
        return logger


setup_logging_subsystem = current_app.log.setup_logging_subsystem
get_default_logger = current_app.log.get_default_logger
setup_logger = current_app.log.setup_logger
setup_task_logger = current_app.log.setup_task_logger
get_task_logger = current_app.log.get_task_logger
redirect_stdouts_to_logger = current_app.log.redirect_stdouts_to_logger


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
