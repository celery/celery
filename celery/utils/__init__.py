from __future__ import generators
"""

Utility functions

"""
import time
import operator
try:
    import ctypes
except ImportError:
    ctypes = None
import importlib
from uuid import UUID, uuid4, _uuid_generate_random
from inspect import getargspec
from itertools import islice

from carrot.utils import rpartition

from celery.utils.compat import all, any, defaultdict
from celery.utils.timeutils import timedelta_seconds # was here before
from celery.utils.functional import curry


def noop(*args, **kwargs):
    """No operation.

    Takes any arguments/keyword arguments and does nothing.

    """
    pass


def kwdict(kwargs):
    """Make sure keyword arguments are not in unicode.

    This should be fixed in newer Python versions,
      see: http://bugs.python.org/issue4978.

    """
    return dict((key.encode("utf-8"), value)
                    for key, value in kwargs.items())


def first(predicate, iterable):
    """Returns the first element in ``iterable`` that ``predicate`` returns a
    ``True`` value for."""
    for item in iterable:
        if predicate(item):
            return item


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
    for first in it:
        yield [first] + list(islice(it, n - 1))


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


def padlist(container, size, default=None):
    """Pad list with default elements.

    Examples:

        >>> first, last, city = padlist(["George", "Costanza", "NYC"], 3)
        ("George", "Costanza", "NYC")
        >>> first, last, city = padlist(["George", "Costanza"], 3)
        ("George", "Costanza", None)
        >>> first, last, city, planet = padlist(["George", "Costanza",
                                                 "NYC"], 4, default="Earth")
        ("George", "Costanza", "NYC", "Earth")

    """
    return list(container)[:size] + [default] * (size - len(container))


def is_iterable(obj):
    try:
        iter(obj)
    except TypeError:
        return False
    return True


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
    while 1: # pragma: no cover
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


def get_cls_by_name(name, aliases={}):
    """Get class by name.

    The name should be the full dot-separated path to the class::

        modulename.ClassName

    Example::

        celery.concurrency.processes.TaskPool
                                    ^- class name

    If ``aliases`` is provided, a dict containing short name/long name
    mappings, the name is looked up in the aliases first.

    Examples:

        >>> get_cls_by_name("celery.concurrency.processes.TaskPool")
        <class 'celery.concurrency.processes.TaskPool'>

        >>> get_cls_by_name("default", {
        ...     "default": "celery.concurrency.processes.TaskPool"})
        <class 'celery.concurrency.processes.TaskPool'>

        # Does not try to look up non-string names.
        >>> from celery.concurrency.processes import TaskPool
        >>> get_cls_by_name(TaskPool) is TaskPool
        True

    """

    if not isinstance(name, basestring):
        return name # already a class

    name = aliases.get(name) or name
    module_name, _, cls_name = rpartition(name, ".")
    module = importlib.import_module(module_name)
    return getattr(module, cls_name)


def instantiate(name, *args, **kwargs):
    """Instantiate class by name.

    See :func:`get_cls_by_name`.

    """
    return get_cls_by_name(name)(*args, **kwargs)
