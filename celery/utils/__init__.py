"""

Utility functions

"""
import time
import operator
try:
    import ctypes
except ImportError:
    ctypes = None
from uuid import UUID, uuid4
try:
    from uuid import _uuid_generate_random
except ImportError:
    _uuid_generate_random = None
from inspect import getargspec
from itertools import repeat

from billiard.utils.functional import curry

noop = lambda *args, **kwargs: None


def chunks(it, n):
    """Split an iterator into chunks with ``n`` elements each.

    Examples

        # n == 2
        >>> x = chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), 2)
        >>> list(x)
        [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9], [10]]

        # n == 3
        >>> x = chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), 3)
        >>> list(x)
        [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]]

    """
    acc = []
    for i, item in enumerate(it):
        if i and not i % n:
            yield acc
            acc = []
        acc.append(item)
    yield acc


def gen_unique_id():
    """Generate a unique id, having - hopefully - a very small chance of
    collission.

    For now this is provided by :func:`uuid.uuid4`.
    """
    # Workaround for http://bugs.python.org/issue4607
    if ctypes and _uuid_generate_random:
        buffer = ctypes.create_string_buffer(16)
        _uuid_generate_random(buffer)
        return str(UUID(bytes=buffer.raw))
    return str(uuid4())


def mitemgetter(*items):
    """Like :func:`operator.itemgetter` but returns ``None`` on missing items
    instead of raising :exc:`KeyError`."""
    return lambda container: map(container.get, items)


def mattrgetter(*attrs):
    """Like :func:`operator.itemgetter` but returns ``None`` on missing
    attributes instead of raising :exc:`AttributeError`."""
    return lambda obj: dict((attr, getattr(obj, attr, None))
                                for attr in attrs)


def get_full_cls_name(cls):
    """With a class, get its full module and class name."""
    return ".".join([cls.__module__,
                     cls.__name__])


def repeatlast(it):
    """Iterate over all elements in the iterator, and when its exhausted
    yield the last value infinitely."""
    for item in it:
        yield item
    for item in repeat(item):
        yield item


def retry_over_time(fun, catch, args=[], kwargs={}, errback=noop,
        max_retries=None, interval_start=2, interval_step=2, interval_max=30):
    """Retry the function over and over until max retries is exceeded.

    For each retry we sleep a for a while before we try again, this interval
    is increased for every retry until the max seconds is reached.

    :param fun: The function to try
    :param catch: Exceptions to catch, can be either tuple or a single
        exception class.
    :keyword args: Positional arguments passed on to the function.
    :keyword kwargs: Keyword arguments passed on to the function.
    :keyword errback: Callback for when an exception in ``catch`` is raised.
        The callback must take two arguments: ``exc`` and ``interval``, where
        ``exc`` is the exception instance, and ``interval`` is the time in
        seconds to sleep next..
    :keyword max_retries: Maximum number of retries before we give up.
        If this is not set, we will retry forever.
    :keyword interval_start: How long (in seconds) we start sleeping between
        retries.
    :keyword interval_step: By how much the interval is increased for each
        retry.
    :keyword interval_max: Maximum number of seconds to sleep between retries.

    """
    retries = 0
    interval_range = xrange(interval_start,
                            interval_max + interval_start,
                            interval_step)

    for interval in repeatlast(interval_range):
        try:
            retval = fun(*args, **kwargs)
        except catch, exc:
            if max_retries and retries > max_retries:
                raise
            errback(exc, interval)
            retries += 1
            time.sleep(interval)
        else:
            return retval


def fun_takes_kwargs(fun, kwlist=[]):
    """With a function, and a list of keyword arguments, returns arguments
    in the list which the function takes.

    If the object has an ``argspec`` attribute that is used instead
    of using the :meth:`inspect.getargspec`` introspection.

    :param fun: The function to inspect arguments of.
    :param kwlist: The list of keyword arguments.

    Examples

        >>> def foo(self, x, y, logfile=None, loglevel=None):
        ...     return x * y
        >>> fun_takes_kwargs(foo, ["logfile", "loglevel", "task_id"])
        ["logfile", "loglevel"]

        >>> def foo(self, x, y, **kwargs):
        >>> fun_takes_kwargs(foo, ["logfile", "loglevel", "task_id"])
        ["logfile", "loglevel", "task_id"]

    """
    argspec = getattr(fun, "argspec", getargspec(fun))
    args, _varargs, keywords, _defaults = argspec
    if keywords != None:
        return kwlist
    return filter(curry(operator.contains, args), kwlist)


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
    collections.defaultdict = defaultdict # Pickle needs this.


try:
    all([True])
    all = all
except NameError:
    def all(iterable):
        for item in iterable:
            if not item:
                return False
        return True


try:
    any([True])
    any = any
except NameError:
    def any(iterable):
        for item in iterable:
            if item:
                return True
        return False


