"""celery.log"""
import os
import sys
import time
import logging
import traceback

from celery import conf
from celery.utils import noop
from celery.utils.patch import ensure_process_aware_logger

_hijacked = False
_monkeypatched = False


def _hijack_multiprocessing_logger():
    from multiprocessing import util as mputil
    global _hijacked

    if _hijacked:
        return mputil.get_logger()

    ensure_process_aware_logger()

    logging.Logger.manager.loggerDict.clear()

    try:
        if mputil._logger is not None:
            mputil.logger = None
    except AttributeError:
        pass

    _hijacked = True
    return mputil.get_logger()


def get_default_logger(loglevel=None):
    """Get default logger instance.

    :keyword loglevel: Initial log level.

    """
    logger = _hijack_multiprocessing_logger()
    if loglevel is not None:
        logger.setLevel(loglevel)
    return logger


def setup_logger(loglevel=conf.CELERYD_LOG_LEVEL, logfile=None,
        format=conf.CELERYD_LOG_FORMAT, **kwargs):
    """Setup the ``multiprocessing`` logger. If ``logfile`` is not specified,
    ``stderr`` is used.

    Returns logger object.
    """

    logger = get_default_logger(loglevel=loglevel)
    if logger.handlers: # Logger already configured
        return logger

    if logfile:
        handler = logging.FileHandler
        if hasattr(logfile, "write"):
            handler = logging.StreamHandler
        loghandler = handler(logfile)
        formatter = logging.Formatter(format)
        loghandler.setFormatter(formatter)
        logger.addHandler(loghandler)
    else:
        from multiprocessing.util import log_to_stderr
        log_to_stderr()
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

    def __init__(self, logger, loglevel=None):
        self.logger = logger
        self.loglevel = loglevel or self.logger.level or self.loglevel
        self._safewrap_handlers()

    def _safewrap_handlers(self):
        """Make the logger handlers dump internal errors to
        ``sys.__stderr__`` instead of ``sys.stderr`` to circumvent
        infinite loops."""

        def wrap_handler(handler): # pragma: no cover

            class WithSafeHandleError(logging.Handler):

                def handleError(self, record):
                    exc_info = sys.exc_info()
                    try:
                        traceback.print_exception(exc_info[0], exc_info[1],
                                                  exc_info[2], None,
                                                  sys.__stderr__)
                    except IOError:
                        pass    # see python issue 5971
                    finally:
                        del(exc_info)

            handler.handleError = WithSafeHandleError().handleError

        return map(wrap_handler, self.logger.handlers)

    def write(self, data):
        """Write message to logging object."""
        if not self.closed:
            self.logger.log(self.loglevel, data)

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
