import time
from collections import deque

from celery.task.base import Task


class CoroutineTask(Task):
    abstract = True
    _current_gen = None

    def body(self):
        while True:
            args, kwargs = (yield)
            yield self.run(*args, *kwargs)

    def run(self, *args, **kwargs):
        try:
            return self._gen.send((args, kwargs))
        finally:
            self._gen.next() # Go to receive-mode

    @property
    def _gen(self):
        if not self._current_gen:
            self._current_gen = self.body()
            self._current_gen.next() # Go to receive-mode
        return self._current_gen


class Aggregate(CoroutineTask):
    abstract = True
    proxied = None
    minlen = 100
    time_max = 60
    _time_since = None

    def body(self):
        waiting = deque()

        while True:
            argtuple = (yield)
            waiting.append(argtuple)
            if self._expired() or len(waiting) >= self.minlen:
                yield self.process(waiting)
                waiting.clear()
            else:
                yield None

    def process(self, jobs):
        """Jobs is a deque with the arguments gathered so far.

        Arguments is a args, kwargs tuple.

        """
        raise NotImplementedError(
                "Subclasses of Aggregate needs to implement process()")

    def _expired(self):
        if not self._time_since:
            self._time_since = time.time()
            return False

        if time.time() + self.time_max > self._time_since:
            self._time_since = time.time()
            return True
        return False
