# -*- coding: utf-8 -*-
"""
    celery.worker.state
    ~~~~~~~~~~~~~~~~~~~

    Internal worker state (global)

    This includes the currently active and reserved tasks,
    statistics, and revoked tasks.

"""
from __future__ import absolute_import

import os
import sys
import platform
import shelve

from collections import defaultdict

from kombu.serialization import pickle_protocol
from kombu.utils import cached_property

from celery import __version__
from celery.datastructures import LimitedSet

#: Worker software/platform information.
SOFTWARE_INFO = {'sw_ident': 'py-celery',
                 'sw_ver': __version__,
                 'sw_sys': platform.system()}

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
total_count = defaultdict(int)

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


C_BENCH = os.environ.get('C_BENCH') or os.environ.get('CELERY_BENCH')
C_BENCH_EVERY = int(os.environ.get('C_BENCH_EVERY') or
                    os.environ.get('CELERY_BENCH_EVERY') or 1000)
if C_BENCH:  # pragma: no cover
    import atexit

    from time import time
    from billiard import current_process
    from celery.utils.debug import memdump, sample_mem

    all_count = 0
    bench_first = None
    bench_start = None
    bench_last = None
    bench_every = C_BENCH_EVERY
    bench_sample = []
    __reserved = task_reserved
    __ready = task_ready

    if current_process()._name == 'MainProcess':
        @atexit.register
        def on_shutdown():
            if bench_first is not None and bench_last is not None:
                print('- Time spent in benchmark: %r' % (
                    bench_last - bench_first))
                print('- Avg: %s' % (sum(bench_sample) / len(bench_sample)))
                memdump()

    def task_reserved(request):  # noqa
        global bench_start
        global bench_first
        now = None
        if bench_start is None:
            bench_start = now = time()
        if bench_first is None:
            bench_first = now

        return __reserved(request)

    def task_ready(request):  # noqa
        global all_count
        global bench_start
        global bench_last
        all_count += 1
        if not all_count % bench_every:
            now = time()
            diff = now - bench_start
            print('- Time spent processing %s tasks (since first '
                  'task received): ~%.4fs\n' % (bench_every, diff))
            sys.stdout.flush()
            bench_start = bench_last = now
            bench_sample.append(diff)
            sample_mem()
        return __ready(request)


class Persistent(object):
    """This is the persistent data stored by the worker when
    :option:`--statedb` is enabled.

    It currently only stores revoked task id's.

    """
    storage = shelve
    protocol = pickle_protocol
    _is_open = False

    def __init__(self, filename):
        self.filename = filename
        self._load()

    def save(self):
        self.sync(self.db)
        self.db.sync()
        self.close()

    def merge(self, d):
        saved = d.get('revoked') or LimitedSet()
        if isinstance(saved, LimitedSet):
            revoked.update(saved)
        else:
            # (pre 3.0.18) used to be stored as dict
            for item in saved:
                revoked.add(item)
        return d

    def sync(self, d):
        revoked.purge()
        d['revoked'] = revoked
        return d

    def open(self):
        return self.storage.open(
            self.filename, protocol=self.protocol, writeback=True,
        )

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
