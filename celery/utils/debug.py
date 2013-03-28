# -*- coding: utf-8 -*-
"""
    celery.utils.debug
    ~~~~~~~~~~~~~~~~~~

    Utilities for debugging memory usage.

"""
from __future__ import absolute_import

import os

from .compat import format_d

try:
    from psutil import Process
except ImportError:
    Process = None  # noqa

_process = None
_mem_sample = []


def sample_mem():
    """Sample RSS memory usage.

    Statistics can then be output by calling :func:`memdump`.

    """
    _mem_sample.append(mem_rss())


def memdump(samples=10):
    """Dump memory statistics.

    Will print a sample of all RSS memory samples added by
    calling :func:`sample_mem`, and in addition print
    used RSS memory after :func:`gc.collect`.

    """
    if ps() is None:
        print('- rss: (psutil not installed).')
        return
    if any(_mem_sample):
        print('- rss (sample):')
        for mem in sample(_mem_sample, samples):
            print('-    > %s,' % mem)
        _mem_sample[:] = []
    import gc
    gc.collect()
    print('- rss (end): %s.' % (mem_rss()))


def sample(x, n, k=0):
    """Given a list `x` a sample of length ``n`` of that list is returned.

    E.g. if `n` is 10, and `x` has 100 items, a list of every 10th
    item is returned.

    ``k`` can be used as offset.

    """
    j = len(x) // n
    for _ in xrange(n):
        yield x[k]
        k += j


def mem_rss():
    """Returns RSS memory usage as a humanized string."""
    p = ps()
    if p is not None:
        return '%sMB' % (format_d(p.get_memory_info().rss // 1024), )


def ps():
    """Returns the global :class:`psutil.Process` instance,
    or :const:`None` if :mod:`psutil` is not installed."""
    global _process
    if _process is None and Process is not None:
        _process = Process(os.getpid())
    return _process
