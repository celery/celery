import threading
from threadpool import ThreadPool, WorkRequest

from celery.concurrency.base import apply_target, BasePool


class TaskPool(BasePool):

    def on_start(self):
        self._pool = ThreadPool(self.limit)

    def on_stop(self):
        self._pool.dismissWorkers(self.limit, do_join=True)

    def on_apply(self, target, args=None, kwargs=None, callback=None,
            accept_callback=None, **_):
        req = WorkRequest(apply_target, (target, args, kwargs, callback,
                                         accept_callback))
        self._pool.putRequest(req)
        # threadpool also has callback support,
        # but for some reason the callback is not triggered
        # before you've collected the results.
        # Clear the results (if any), so it doesn't grow too large.
        self._pool._results_queue.queue.clear()
        return req
