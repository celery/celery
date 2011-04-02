from __future__ import generators

############## py3k #########################################################
try:
    from UserList import UserList
except ImportError:
    from collections import UserList

try:
    from UserDict import UserDict
except ImportError:
    from collections import UserDict

try:
    from cStringIO import StringIO
except ImportError:
    try:
        from StringIO import StringIO
    except ImportError:
        from io import StringIO

############## urlparse.parse_qsl ###########################################

try:
    from urlparse import parse_qsl
except ImportError:
    from cgi import parse_qsl
parse_sql = parse_qsl

############## __builtin__.all ##############################################

try:
    all([True])
    all = all
except NameError:
    def all(iterable):
        for item in iterable:
            if not item:
                return False
        return True

############## __builtin__.any ##############################################

try:
    any([True])
    any = any
except NameError:
    def any(iterable):
        for item in iterable:
            if item:
                return True
        return False

############## collections.OrderedDict ######################################

import weakref
try:
    from collections import MutableMapping
except ImportError:
    from UserDict import DictMixin as MutableMapping
from itertools import imap as _imap
from operator import eq as _eq


class _Link(object):
    """Doubly linked list."""
    # next can't be lowercase because 2to3 thinks it's a generator
    # and renames it to __next__.
    __slots__ = 'PREV', 'NEXT', 'key', '__weakref__'


class CompatOrderedDict(dict, MutableMapping):
    """Dictionary that remembers insertion order"""
    # An inherited dict maps keys to values.
    # The inherited dict provides __getitem__, __len__, __contains__, and get.
    # The remaining methods are order-aware.
    # Big-O running times for all methods are the same as for regular
    # dictionaries.

    # The internal self.__map dictionary maps keys to links in a doubly
    # linked list.
    # The circular doubly linked list starts and ends with a sentinel element.
    # The sentinel element never gets deleted (this simplifies the algorithm).
    # The prev/next links are weakref proxies (to prevent circular
    # references).
    # Individual links are kept alive by the hard reference in self.__map.
    # Those hard references disappear when a key is deleted from
    # an OrderedDict.

    __marker = object()

    def __init__(self, *args, **kwds):
        """Initialize an ordered dictionary.

        Signature is the same as for regular dictionaries, but keyword
        arguments are not recommended because their insertion order is
        arbitrary.

        """
        if len(args) > 1:
            raise TypeError("expected at most 1 arguments, got %d" % (
                                len(args)))
        try:
            self._root
        except AttributeError:
            # sentinel node for the doubly linked list
            self._root = root = _Link()
            root.PREV = root.NEXT = root
            self.__map = {}
        self.update(*args, **kwds)

    def clear(self):
        "od.clear() -> None.  Remove all items from od."
        root = self._root
        root.PREV = root.NEXT = root
        self.__map.clear()
        dict.clear(self)

    def __setitem__(self, key, value):
        "od.__setitem__(i, y) <==> od[i]=y"
        # Setting a new item creates a new link which goes at the end of the
        # linked list, and the inherited dictionary is updated with the new
        # key/value pair.
        if key not in self:
            self.__map[key] = link = _Link()
            root = self._root
            last = root.PREV
            link.PREV, link.NEXT, link.key = last, root, key
            last.NEXT = root.PREV = weakref.proxy(link)
        dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        """od.__delitem__(y) <==> del od[y]"""
        # Deleting an existing item uses self.__map to find the
        # link which is then removed by updating the links in the
        # predecessor and successor nodes.
        dict.__delitem__(self, key)
        link = self.__map.pop(key)
        link.PREV.NEXT = link.NEXT
        link.NEXT.PREV = link.PREV

    def __iter__(self):
        """od.__iter__() <==> iter(od)"""
        # Traverse the linked list in order.
        root = self._root
        curr = root.NEXT
        while curr is not root:
            yield curr.key
            curr = curr.NEXT

    def __reversed__(self):
        """od.__reversed__() <==> reversed(od)"""
        # Traverse the linked list in reverse order.
        root = self._root
        curr = root.PREV
        while curr is not root:
            yield curr.key
            curr = curr.PREV

    def __reduce__(self):
        """Return state information for pickling"""
        items = [[k, self[k]] for k in self]
        tmp = self.__map, self._root
        del(self.__map, self._root)
        inst_dict = vars(self).copy()
        self.__map, self._root = tmp
        if inst_dict:
            return (self.__class__, (items,), inst_dict)
        return self.__class__, (items,)

    def setdefault(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            self[key] = default
        return default

    def update(self, other=(), **kwds):
        if isinstance(other, dict):
            for key in other:
                self[key] = other[key]
        elif hasattr(other, "keys"):
            for key in other.keys():
                self[key] = other[key]
        else:
            for key, value in other:
                self[key] = value
        for key, value in kwds.items():
            self[key] = value

    def pop(self, key, default=__marker):
        try:
            value = self[key]
        except KeyError:
            if default is self.__marker:
                raise
            return default
        else:
            del self[key]
            return value

    def values(self):
        return [self[key] for key in self]

    def items(self):
        return [(key, self[key]) for key in self]

    def itervalues(self):
        for key in self:
            yield self[key]

    def iteritems(self):
        for key in self:
            yield (key, self[key])

    def iterkeys(self):
        return iter(self)

    def keys(self):
        return list(self)

    def popitem(self, last=True):
        """od.popitem() -> (k, v)

        Return and remove a (key, value) pair.
        Pairs are returned in LIFO order if last is true or FIFO
        order if false.

        """
        if not self:
            raise KeyError('dictionary is empty')
        key = (last and reversed(self) or iter(self)).next()
        value = self.pop(key)
        return key, value

    def __repr__(self):
        "od.__repr__() <==> repr(od)"
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, self.items())

    def copy(self):
        "od.copy() -> a shallow copy of od"
        return self.__class__(self)

    @classmethod
    def fromkeys(cls, iterable, value=None):
        """OD.fromkeys(S[, v]) -> New ordered dictionary with keys from S
        and values equal to v (which defaults to None)."""
        d = cls()
        for key in iterable:
            d[key] = value
        return d

    def __eq__(self, other):
        """od.__eq__(y) <==> od==y.  Comparison to another OD is
        order-sensitive while comparison to a regular mapping
        is order-insensitive."""
        if isinstance(other, OrderedDict):
            return len(self) == len(other) and \
                   all(_imap(_eq, self.iteritems(), other.iteritems()))
        return dict.__eq__(self, other)

    def __ne__(self, other):
        return not (self == other)

try:
    from collections import OrderedDict
except ImportError:
    OrderedDict = CompatOrderedDict

############## collections.defaultdict ######################################

try:
    from collections import defaultdict
except ImportError:
    # Written by Jason Kirtland, taken from Python Cookbook:
    # <http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/523034>
    class defaultdict(dict):

        def __init__(self, default_factory=None, *args, **kwargs):
            dict.__init__(self, *args, **kwargs)
            self.default_factory = default_factory

        def __getitem__(self, key):
            try:
                return dict.__getitem__(self, key)
            except KeyError:
                return self.__missing__(key)

        def __missing__(self, key):
            if self.default_factory is None:
                raise KeyError(key)
            self[key] = value = self.default_factory()
            return value

        def __reduce__(self):
            f = self.default_factory
            args = f is None and tuple() or f
            return type(self), args, None, None, self.iteritems()

        def copy(self):
            return self.__copy__()

        def __copy__(self):
            return type(self)(self.default_factory, self)

        def __deepcopy__(self):
            import copy
            return type(self)(self.default_factory,
                        copy.deepcopy(self.items()))

        def __repr__(self):
            return "defaultdict(%s, %s)" % (self.default_factory,
                                            dict.__repr__(self))
    import collections
    collections.defaultdict = defaultdict           # Pickle needs this.

############## logging.LoggerAdapter ########################################
import inspect
import logging
try:
    import multiprocessing
except ImportError:
    multiprocessing = None
import sys

from logging import LogRecord

log_takes_extra = "extra" in inspect.getargspec(logging.Logger._log)[0]

# The func argument to LogRecord was added in 2.5
if "func" not in inspect.getargspec(LogRecord.__init__)[0]:
    def LogRecord(name, level, fn, lno, msg, args, exc_info, func):
        return logging.LogRecord(name, level, fn, lno, msg, args, exc_info)


def _checkLevel(level):
    if isinstance(level, int):
        rv = level
    elif str(level) == level:
        if level not in logging._levelNames:
            raise ValueError("Unknown level: %r" % level)
        rv = logging._levelNames[level]
    else:
        raise TypeError("Level not an integer or a valid string: %r" % level)
    return rv


class _CompatLoggerAdapter(object):

    def __init__(self, logger, extra):
        self.logger = logger
        self.extra = extra

    def setLevel(self, level):
        self.logger.level = _checkLevel(level)

    def process(self, msg, kwargs):
        kwargs["extra"] = self.extra
        return msg, kwargs

    def debug(self, msg, *args, **kwargs):
        self.log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.log(logging.WARNING, msg, *args, **kwargs)
    warn = warning

    def error(self, msg, *args, **kwargs):
        self.log(logging.ERROR, msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        kwargs.setdefault("exc_info", 1)
        self.error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.log(logging.CRITICAL, msg, *args, **kwargs)
    fatal = critical

    def log(self, level, msg, *args, **kwargs):
        if self.logger.isEnabledFor(level):
            msg, kwargs = self.process(msg, kwargs)
            self._log(level, msg, args, **kwargs)

    def makeRecord(self, name, level, fn, lno, msg, args, exc_info,
            func=None, extra=None):
        rv = LogRecord(name, level, fn, lno, msg, args, exc_info, func)
        if extra is not None:
            for key, value in extra.items():
                if key in ("message", "asctime") or key in rv.__dict__:
                    raise KeyError(
                            "Attempt to override %r in LogRecord" % key)
                rv.__dict__[key] = value
        if multiprocessing is not None:
            rv.processName = multiprocessing.current_process()._name
        else:
            rv.processName = ""
        return rv

    def _log(self, level, msg, args, exc_info=None, extra=None):
        defcaller = "(unknown file)", 0, "(unknown function)"
        if logging._srcfile:
            # IronPython doesn't track Python frames, so findCaller
            # throws an exception on some versions of IronPython.
            # We trap it here so that IronPython can use logging.
            try:
                fn, lno, func = self.logger.findCaller()
            except ValueError:
                fn, lno, func = defcaller
        else:
            fn, lno, func = defcaller
        if exc_info:
            if not isinstance(exc_info, tuple):
                exc_info = sys.exc_info()
        record = self.makeRecord(self.logger.name, level, fn, lno, msg,
                                 args, exc_info, func, extra)
        self.logger.handle(record)

    def isEnabledFor(self, level):
        return self.logger.isEnabledFor(level)

    def addHandler(self, hdlr):
        self.logger.addHandler(hdlr)

    def removeHandler(self, hdlr):
        self.logger.removeHandler(hdlr)

    @property
    def level(self):
        return self.logger.level


try:
    from logging import LoggerAdapter
except ImportError:
    LoggerAdapter = _CompatLoggerAdapter


def log_with_extra(logger, level, msg, *args, **kwargs):
    if not log_takes_extra:
        kwargs.pop("extra", None)
    return logger.log(level, msg, *args, **kwargs)

############## itertools.izip_longest #######################################

try:
    from itertools import izip_longest
except ImportError:
    import itertools

    def izip_longest(*args, **kwds):
        fillvalue = kwds.get("fillvalue")

        def sentinel(counter=([fillvalue] * (len(args) - 1)).pop):
            yield counter()     # yields the fillvalue, or raises IndexError

        fillers = itertools.repeat(fillvalue)
        iters = [itertools.chain(it, sentinel(), fillers)
                    for it in args]
        try:
            for tup in itertools.izip(*iters):
                yield tup
        except IndexError:
            pass


############## itertools.chain.from_iterable ################################
from itertools import chain


def _compat_chain_from_iterable(iterables):
    for it in iterables:
        for element in it:
            yield element

try:
    chain_from_iterable = getattr(chain, "from_iterable")
except AttributeError:
    chain_from_iterable = _compat_chain_from_iterable


############## logging.handlers.WatchedFileHandler ##########################
import os
from stat import ST_DEV, ST_INO
import platform as _platform

if _platform.system() == "Windows":
    #since windows doesn't go with WatchedFileHandler use FileHandler instead
    WatchedFileHandler = logging.FileHandler
else:
    try:
        from logging.handlers import WatchedFileHandler
    except ImportError:
        class WatchedFileHandler(logging.FileHandler):
            """
            A handler for logging to a file, which watches the file
            to see if it has changed while in use. This can happen because of
            usage of programs such as newsyslog and logrotate which perform
            log file rotation. This handler, intended for use under Unix,
            watches the file to see if it has changed since the last emit.
            (A file has changed if its device or inode have changed.)
            If it has changed, the old file stream is closed, and the file
            opened to get a new stream.

            This handler is not appropriate for use under Windows, because
            under Windows open files cannot be moved or renamed - logging
            opens the files with exclusive locks - and so there is no need
            for such a handler. Furthermore, ST_INO is not supported under
            Windows; stat always returns zero for this value.

            This handler is based on a suggestion and patch by Chad J.
            Schroeder.
            """
            def __init__(self, *args, **kwargs):
                logging.FileHandler.__init__(self, *args, **kwargs)

                if not os.path.exists(self.baseFilename):
                    self.dev, self.ino = -1, -1
                else:
                    stat = os.stat(self.baseFilename)
                    self.dev, self.ino = stat[ST_DEV], stat[ST_INO]

            def emit(self, record):
                """
                Emit a record.

                First check if the underlying file has changed, and if it
                has, close the old stream and reopen the file to get the
                current stream.
                """
                if not os.path.exists(self.baseFilename):
                    stat = None
                    changed = 1
                else:
                    stat = os.stat(self.baseFilename)
                    changed = ((stat[ST_DEV] != self.dev) or
                               (stat[ST_INO] != self.ino))
                if changed and self.stream is not None:
                    self.stream.flush()
                    self.stream.close()
                    self.stream = self._open()
                    if stat is None:
                        stat = os.stat(self.baseFilename)
                    self.dev, self.ino = stat[ST_DEV], stat[ST_INO]
                logging.FileHandler.emit(self, record)
