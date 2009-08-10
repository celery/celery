"""

Utility functions

"""
import uuid
import time
from itertools import repeat

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
    return str(uuid.uuid4())


def mitemgetter(*keys):
    """Like :func:`operator.itemgetter` but returns `None` on missing keys
    instead of raising :exc:`KeyError`."""
    return lambda dict_: map(dict_.get, keys)


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
