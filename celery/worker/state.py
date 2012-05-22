# -*- coding: utf-8 -*-
"""
    celery.worker.state
    ~~~~~~~~~~~~~~~~~~~

    Internal worker state (global)

    This includes the currently active and reserved tasks,
    statistics, and revoked tasks.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import os
import platform
import shelve

from collections import defaultdict

from celery import __version__
from celery.datastructures import LimitedSet
from celery.utils import cached_property

#: Worker software/platform information.
SOFTWARE_INFO = {"sw_ident": "celeryd",
                 "sw_ver": __version__,
                 "sw_sys": platform.system()}

#: maximum number of revokes to keep in memory.
REVOKES_MAX = 10000

#: how many seconds a revoke will be active before
#: being expired when the max limit has been exceeded.
REVOKE_EXPIRES = 3600

#: set of all reserved :class:`~celery.worker.job.Request`'s.
reserved_requests = set()

#: set of currently active :class:`~celery.worker.job.Request`'s.
active_requests = set()

#: count of tasks executed by the worker, sorted by type.
total_count = defaultdict(lambda: 0)

#: the list of currently revoked tasks.  Persistent if statedb set.
revoked = LimitedSet(maxlen=REVOKES_MAX, expires=REVOKE_EXPIRES)

#: Updates global state when a task has been reserved.
task_reserved = reserved_requests.add

should_stop = False
should_terminate = False


def task_accepted(request):
    """Updates global state when a task has been accepted."""
    active_requests.add(request)
    total_count[request.name] += 1


def task_ready(request):
    """Updates global state when a task is ready."""
    active_requests.discard(request)
    reserved_requests.discard(request)


if os.environ.get("CELERY_BENCH"):  # pragma: no cover
    import atexit

    from time import time
    from billiard import current_process
    from celery.utils.compat import format_d

    all_count = 0
    bench_first = None
    bench_mem_sample = []
    bench_start = None
    bench_last = None
    bench_every = int(os.environ.get("CELERY_BENCH_EVERY", 1000))
    bench_sample = []
    __reserved = task_reserved
    __ready = task_ready
    _process = None

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

    if current_process()._name == 'MainProcess':
        @atexit.register
        def on_shutdown():
            if bench_first is not None and bench_last is not None:
                print("- Time spent in benchmark: %r" % (
                    bench_last - bench_first))
                print("- Avg: %s" % (sum(bench_sample) / len(bench_sample)))
                if filter(None, bench_mem_sample):
                    print("- rss (sample):")
                    for mem in sample(bench_mem_sample):
                        print("-    > %s," % mem)
                    bench_mem_sample[:] = []
                    bench_sample[:] = []
                    import gc
                    gc.collect()
                    print("- rss (shutdown): %s." % (mem_rss()))
                else:
                    print("- rss: (psutil not installed).")

    def task_reserved(request):  # noqa
        global bench_start
        global bench_first
        now = None
        if bench_start is None:
            bench_start = now = time()
        if bench_first is None:
            bench_first = now

        return __reserved(request)

    import sys
    def task_ready(request):  # noqa
        global all_count
        global bench_start
        global bench_last
        all_count += 1
        if not all_count % bench_every:
            now = time()
            diff = now - bench_start
            print("- Time spent processing %s tasks (since first "
                    "task received): ~%.4fs\n" % (bench_every, diff))
            sys.stdout.flush()
            bench_start = bench_last = now
            bench_sample.append(diff)
            bench_mem_sample.append(mem_rss())

        return __ready(request)


class Persistent(object):
    storage = shelve
    _is_open = False

    def __init__(self, filename):
        self.filename = filename
        self._load()

    def save(self):
        self.sync(self.db)
        self.db.sync()
        self.close()

    def merge(self, d):
        revoked.update(d.get("revoked") or {})
        return d

    def sync(self, d):
        prev = d.get("revoked") or {}
        prev.update(revoked.as_dict())
        d["revoked"] = prev
        return d

    def open(self):
        return self.storage.open(self.filename, writeback=True)

    def close(self):
        if self._is_open:
            self.db.close()
            self._is_open = False

    def _load(self):
        self.merge(self.db)

    @cached_property
    def db(self):
        self._is_open = True
        return self.open()
