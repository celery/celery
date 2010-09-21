"""celery.log"""
import logging
import threading
import time
import os
import sys
import traceback

from multiprocessing import current_process
from multiprocessing import util as mputil

from celery import conf
from celery import signals
from celery.utils import noop
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

    def format(self, record):
        levelname = record.levelname

        if self.use_color and levelname in COLORS:
            record.msg = str(colored().names[COLORS[levelname]](record.msg))

        # Very ugly, but have to make sure processName is supported
        # by foreign logger instances.
        # (processName is always supported by Python 2.7)
        if "processName" not in record.__dict__:
            record.__dict__["processName"] = current_process()._name
        return logging.Formatter.format(self, record)


def get_task_logger(loglevel=None, name=None):
    logger = logging.getLogger(name or "celery.task.default")
    if loglevel is not None:
        logger.setLevel(loglevel)
    return logger


def setup_logging_subsystem(loglevel=conf.CELERYD_LOG_LEVEL, logfile=None,
        format=conf.CELERYD_LOG_FORMAT, colorize=conf.CELERYD_LOG_COLOR,
        **kwargs):
    global _setup
    if not _setup:
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
                _setup_logger(logger, logfile, format, colorize, **kwargs)
                logger.setLevel(loglevel)
        _setup = True
        return receivers


def _detect_handler(logfile=None):
    """Create log handler with either a filename, an open stream
    or ``None`` (stderr)."""
    if not logfile or hasattr(logfile, "write"):
        return logging.StreamHandler(logfile)
    return logging.FileHandler(logfile)


def get_default_logger(loglevel=None, name="celery"):
    """Get default logger instance.

    :keyword loglevel: Initial log level.

    """
    logger = logging.getLogger(name)
    if loglevel is not None:
        logger.setLevel(loglevel)
    return logger


def setup_logger(loglevel=conf.CELERYD_LOG_LEVEL, logfile=None,
        format=conf.CELERYD_LOG_FORMAT, colorize=conf.CELERYD_LOG_COLOR,
        name="celery", root=True, **kwargs):
    """Setup the ``multiprocessing`` logger. If ``logfile`` is not specified,
    then ``stderr`` is used.

    Returns logger object.

    """
    if not root:
        return _setup_logger(get_default_logger(loglevel, name),
                             logfile, format, colorize, **kwargs)
    setup_logging_subsystem(loglevel, logfile, format, colorize, **kwargs)
    return get_default_logger(name=name)


def setup_task_logger(loglevel=conf.CELERYD_LOG_LEVEL, logfile=None,
        format=conf.CELERYD_TASK_LOG_FORMAT, colorize=conf.CELERYD_LOG_COLOR,
        task_kwargs=None, **kwargs):
    """Setup the task logger. If ``logfile`` is not specified, then
    ``stderr`` is used.

    Returns logger object.

    """
    if task_kwargs is None:
        task_kwargs = {}
    task_kwargs.setdefault("task_id", "-?-")
    task_name = task_kwargs.get("task_name")
    task_kwargs.setdefault("task_name", "-?-")
    logger = _setup_logger(get_task_logger(loglevel, task_name),
                            logfile, format, colorize, **kwargs)
    return LoggerAdapter(logger, task_kwargs)


def _setup_logger(logger, logfile, format, colorize,
        formatter=ColorFormatter, **kwargs):

    if logger.handlers:                 # already configured
        return logger

    handler = _detect_handler(logfile)
    handler.setFormatter(formatter(format, use_color=colorize))
    logger.addHandler(handler)
    return logger


def emergency_error(logfile, message):
    """Emergency error logging, for when there's no standard file
    descriptors open because the process has been daemonized or for
    some other reason."""
    closefh = noop
    logfile = logfile or sys.__stderr__
    if hasattr(logfile, "write"):
        logfh = logfile
    else:
        logfh = open(logfile, "a")
        closefh = logfh.close
    try:
        logfh.write("[%(asctime)s: CRITICAL/%(pid)d]: %(message)s\n" % {
                        "asctime": time.asctime(),
                        "pid": os.getpid(),
                        "message": message})
    finally:
        closefh()


def redirect_stdouts_to_logger(logger, loglevel=None):
    """Redirect :class:`sys.stdout` and :class:`sys.stderr` to a
    logging instance.

    :param logger: The :class:`logging.Logger` instance to redirect to.
    :param loglevel: The loglevel redirected messages will be logged as.

    """
    proxy = LoggingProxy(logger, loglevel)
    sys.stdout = sys.stderr = proxy
    return proxy


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
        self._safewrap_handlers()

    def _safewrap_handlers(self):
        """Make the logger handlers dump internal errors to
        ``sys.__stderr__`` instead of ``sys.stderr`` to circumvent
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
            # logger is logging back to this file, so stop recursing.
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
        """``writelines(sequence_of_strings) -> None``.

        Write the strings to the file.

        The sequence can be any iterable object producing strings.
        This is equivalent to calling :meth:`write` for each string.

        """
        map(self.write, sequence)

    def flush(self):
        """This object is not buffered so any :meth:`flush` requests
        are ignored."""
        pass

    def close(self):
        """When the object is closed, no write requests are forwarded to
        the logging object anymore."""
        self.closed = True

    def isatty(self):
        """Always returns ``False``. Just here for file support."""
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
            map(self.action, msgs)
            self._iterations = 0
        else:
            self._iterations += 1
