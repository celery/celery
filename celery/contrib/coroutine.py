from celery.task.base import Task
from celery.task.builtins import PingTask


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

    def body(self):
        waiting = []

        while True:
            argtuple = (yield)
            waiting.append(argtuple)
            if len(waiting) >= self.minlen:
                res = []
                for task_args, task_kwargs in waiting:
                    try:
                        res.append(self.proxied(*args, **kwargs))
                    except Exception, exc:
                        pass # TODO handle errors here, please
                yield res
            else:
                yield None


class AggregatePing(Aggregate):
    proxied = PingTask
    n = 100
