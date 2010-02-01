from itertools import count
from collections import deque, defaultdict

from celery.task.base import Task


class Batches(Task):
    abstract = True
    flush_every = 10

    def __init__(self):
        self._buffer = deque()
        self._count = count().next

    def execute(self, wrapper, pool, loglevel, logfile):
        self._buffer.append((wrapper, pool, loglevel, logfile))

        if not self._count() % self.flush_every:
            self.flush(self._buffer)
            self._buffer.clear()

    def flush(self, tasks):
        for wrapper, pool, loglevel, logfile in tasks:
            wrapper.execute_using_pool(pool, loglevel, logfile)


class Counter(Task):
    abstract = True
    flush_every = 10

    def __init__(self):
        self._buffer = deque()
        self._count = count().next

    def execute(self, wrapper, pool, loglevel, logfile):
        self._buffer.append((wrapper.args, wrapper.kwargs))

        if not self._count() % self.flush_every:
            self.flush(self._buffer)
            self._buffer.clear()

    def flush(self, buffer):
        raise NotImplementedError("Counters must implement 'flush'")


class ClickCounter(Task):
    flush_every = 1000

    def flush(self, buffer):
        urlcount = defaultdict(lambda: 0)
        for args, kwargs in buffer:
            urlcount[kwargs["url"]] += 1

        for url, count in urlcount.items():
            print(">>> Clicks: %s -> %s" % (url, count))
            # increment_in_db(url, n=count)
