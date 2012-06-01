from __future__ import absolute_import

import os

from .compat import format_d

_process = None
_mem_sample = []

def ps():
    global _process
    if _process is None:
        try:
            from psutil import Process
        except ImportError:
            return None
        _process = Process(os.getpid())
    return _process

def mem_rss():
    p = ps()
    if p is not None:
        return "%sMB" % (format_d(p.get_memory_info().rss // 1024), )

def sample(x, n=10, k=0):
    j = len(x) // n
    for _ in xrange(n):
        yield x[k]
        k += j


def memdump():
    if ps() is None:
        print("- rss: (psutil not installed).")
        return
    if filter(None, _mem_sample):
        print("- rss (sample):")
        for mem in sample(_mem_sample):
            print("-    > %s," % mem)
        _mem_sample[:] = []
    import gc
    gc.collect()
    print("- rss (end): %s." % (mem_rss()))


def sample_mem():
    _mem_sample.append(mem_rss())
