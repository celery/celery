"""Logging utilities."""
import logging
import numbers
import os
import sys
import threading
import traceback
from contextlib import contextmanager
from typing import AnyStr, Sequence  # noqa

from kombu.log import LOG_LEVELS
from kombu.log import get_logger as _get_logger
from kombu.utils.encoding import safe_str

from .term import colored

__all__ = (
    'ColorFormatter', 'LoggingProxy', 'base_logger',
    'set_in_sighandler', 'in_sighandler', 'get_logger',
    'get_task_logger', 'mlevel',
    'get_multiprocessing_logger', 'reset_multiprocessing_logger', 'LOG_LEVELS'
)

_process_aware = False
_in_sighandler = False

MP_LOG = os.environ.get('MP_LOG', False)

RESERVED_LOGGER_NAMES = {'celery', 'celery.task'}

# Sets up our logging hierarchy.
#
# Every logger in the celery package inherits from the "celery"
# logger, and every task logger inherits from the "celery.task"
# logger.
base_logger = logger = _get_logger('celery')


def set_in_sighandler(value):
    """Set flag signifying that we're inside a signal handler."""
    global _in_sighandler
    _in_sighandler = value


def iter_open_logger_fds():
    seen = set()
    loggers = (list(logging.Logger.manager.loggerDict.values()) +
               [logging.getLogger(None)])
    for l in loggers:
        try:
            for handler in l.handlers:
                try:
                    if handler not in seen:  # pragma: no cover
                        yield handler.stream
                        seen.add(handler)
                except AttributeError:
                    pass
        except AttributeError:  # PlaceHolder does not have handlers
            pass


@contextmanager
def in_sighandler():
    """Context that records that we are in a signal handler."""
    set_in_sighandler(True)
    try:
        yield
    finally:
        set_in_sighandler(False)


def logger_isa(l, p, max=1000):
    this, seen = l, set()
    for _ in range(max):
        if this == p:
            return True
        else:
            if this in seen:
                raise RuntimeError(
                    f'Logger {l.name!r} parents recursive',
                )
            seen.add(this)
            this = this.parent
            if not this:
                break
    else:  # pragma: no cover
        raise RuntimeError(f'Logger hierarchy exceeds {max}')
    return False


def _using_logger_parent(parent_logger, logger_):
    if not logger_isa(logger_, parent_logger):
        logger_.parent = parent_logger
    return logger_


def get_logger(name):
    """Get logger by name."""
    l = _get_logger(name)
    if logging.root not in (l, l.parent) and l is not base_logger:
        l = _using_logger_parent(base_logger, l)
    return l


task_logger = get_logger('celery.task')
worker_logger = get_logger('celery.worker')


def get_task_logger(name):
    """Get logger for task module by name."""
    if name in RESERVED_LOGGER_NAMES:
        raise RuntimeError(f'Logger name {name!r} is reserved!')
    return _using_logger_parent(task_logger, get_logger(name))


def mlevel(level):
    """Convert level name/int to log level."""
    if level and not isinstance(level, numbers.Integral):
        return LOG_LEVELS[level.upper()]
    return level


class ColorFormatter(logging.Formatter):
    """Logging formatter that adds colors based on severity."""

    #: Loglevel -> Color mapping.
    COLORS = colored().names
    colors = {
        'DEBUG': COLORS['blue'],
        'WARNING': COLORS['yellow'],
        'ERROR': COLORS['red'],
        'CRITICAL': COLORS['magenta'],
    }

    def __init__(self, fmt=None, use_color=True, datefmt=None):
        super().__init__(fmt, datefmt)
        self.use_color = use_color

    def formatException(self, ei):
        if ei and not isinstance(ei, tuple):
            ei = sys.exc_info()
        r = super().formatException(ei)
        return r

    def format(self, record):
        msg = super().format(record)
        color = self.colors.get(record.levelname)

        # reset exception info later for other handlers...
        einfo = sys.exc_info() if record.exc_info == 1 else record.exc_info

        if color and self.use_color:
            try:
                # safe_str will repr the color object
                # and color will break on non-string objects
                # so need to reorder calls based on type.
                # Issue #427
                try:
                    if isinstance(msg, str):
                        return str(color(safe_str(msg)))
                    return safe_str(color(msg))
                except UnicodeDecodeError:  # pragma: no cover
                    return safe_str(msg)  # skip colors
            except Exception as exc:  # pylint: disable=broad-except
                prev_msg, record.exc_info, record.msg = (
                    record.msg, 1, '<Unrepresentable {!r}: {!r}>'.format(
                        type(msg), exc
                    ),
                )
                try:
                    return super().format(record)
                finally:
                    record.msg, record.exc_info = prev_msg, einfo
        else:
            return safe_str(msg)


class LoggingProxy:
    """Forward file object to :class:`logging.Logger` instance.

    Writes are line buffered: a message is only logged once a trailing
    newline arrives, on :meth:`flush` or on :meth:`close`. Callers such as
    :mod:`traceback` write a single line in several pieces, and logging each
    piece separately turns one line into several log records.

    Arguments:
        logger (~logging.Logger): Logger instance to forward to.
        loglevel (int, str): Log level to use when logging messages.
    """

    mode = 'w'
    name = None
    closed = False
    loglevel = logging.ERROR
    _thread = threading.local()

    def __init__(self, logger, loglevel=None):
        # pylint: disable=redefined-outer-name
        # Note that the logger global is redefined here, be careful changing.
        self.logger = logger
        self.loglevel = mlevel(loglevel or self.logger.level or self.loglevel)
        # Buffer per instance and per thread, so that stdout and stderr, and
        # two threads writing at once, cannot interleave into one message.
        self._local = threading.local()
        self._safewrap_handlers()

    def _safewrap_handlers(self):
        # Make the logger handlers dump internal errors to
        # :data:`sys.__stderr__` instead of :data:`sys.stderr` to circumvent
        # infinite loops.

        def wrap_handler(handler):                  # pragma: no cover

            class WithSafeHandleError(logging.Handler):

                def handleError(self, record):
                    try:
                        traceback.print_exc(None, sys.__stderr__)
                    except OSError:
                        pass    # see python issue 5971

            handler.handleError = WithSafeHandleError().handleError
        return [wrap_handler(h) for h in self.logger.handlers]

    @property
    def _buffer(self):
        # type: () -> list
        try:
            return self._local.buffer
        except AttributeError:
            self._local.buffer = []
            return self._local.buffer

    def write(self, data):
        # type: (AnyStr) -> int
        """Buffer message, and log it once the line is complete."""
        if _in_sighandler:
            safe_data = safe_str(data)
            print(safe_data, file=sys.__stderr__)
            return len(safe_data)
        if getattr(self._thread, 'recurse_protection', False):
            # Logger is logging back to this file, so stop recursing.
            return 0
        if data and not self.closed:
            safe_data = safe_str(data)
            self._buffer.append(safe_data)
            if safe_data.endswith('\n'):
                self._flush_buffer()
            return len(safe_data)
        return 0

    def _flush_buffer(self):
        # type: () -> None
        if getattr(self._thread, 'recurse_protection', False):
            # Logger is logging back to this file. Leave the buffer alone so
            # that a later flush can still log it.
            return
        buffer = self._buffer
        if not buffer:
            return
        data = ''.join(buffer).rstrip('\n')
        del buffer[:]
        if not data or self.closed:
            return
        self._thread.recurse_protection = True
        try:
            self.logger.log(self.loglevel, data)
        finally:
            self._thread.recurse_protection = False

    def writelines(self, sequence):
        # type: (Sequence[str]) -> None
        """Write list of strings to file.

        The sequence can be any iterable object producing strings.
        This is equivalent to calling :meth:`write` for each string.
        """
        for part in sequence:
            self.write(part)

    def flush(self):
        # Log whatever has been written so far without waiting for the
        # trailing newline. The interpreter calls this on sys.stdout and
        # sys.stderr at shutdown, so a partial line is never lost.
        self._flush_buffer()

    def close(self):
        # when the object is closed, no write requests are
        # forwarded to the logging object anymore. Only the calling thread's
        # buffer is flushed, so a partial line another thread is still
        # writing is dropped, as it is for any file closed while in use.
        self._flush_buffer()
        if self._buffer:
            # A recursive logging path on this thread blocked the flush, and
            # closing means there will be no later one. Fall back to the real
            # stderr, as write() does under a signal handler.
            print(''.join(self._buffer).rstrip('\n'), file=sys.__stderr__)
            del self._buffer[:]
        self.closed = True

    def isatty(self):
        """Here for file support."""
        return False


def get_multiprocessing_logger():
    """Return the multiprocessing logger."""
    try:
        from billiard import util
    except ImportError:
        pass
    else:
        return util.get_logger()


def reset_multiprocessing_logger():
    """Reset multiprocessing logging setup."""
    try:
        from billiard import util
    except ImportError:
        pass
    else:
        if hasattr(util, '_logger'):  # pragma: no cover
            util._logger = None


def current_process():
    try:
        from billiard import process
    except ImportError:
        pass
    else:
        return process.current_process()


def current_process_index(base=1):
    index = getattr(current_process(), 'index', None)
    return index + base if index is not None else index
