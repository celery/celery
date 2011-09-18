from __future__ import absolute_import

from .base import apply_target, BasePool

class BlackholeDictionary(object):
    
    def __setitem__(self, k, v):
        pass

    def __delitem__(self, k):
        pass

    def __getitem__(self, k):
        raise Exception("Unsupported operation on blackhole dictionary")


class TaskPool(BasePool):

    def __init__(self, *args, **kwargs):
        try:
            import threadpool
        except ImportError:
            raise ImportError(
                    "The threaded pool requires the threadpool module.")
        self.WorkRequest = threadpool.WorkRequest
        self.ThreadPool = threadpool.ThreadPool
        super(TaskPool, self).__init__(*args, **kwargs)

    def on_start(self):
        self._pool = self.ThreadPool(self.limit)
        # threadpool stores all work requests until they are processed
        # lets overwrite the dict because they are never processed
        self._pool.workRequests = BlackholeDictionary()

    def on_stop(self):
        self._pool.dismissWorkers(self.limit, do_join=True)

    def on_apply(self, target, args=None, kwargs=None, callback=None,
            accept_callback=None, **_):
        req = self.WorkRequest(apply_target, (target, args, kwargs, callback,
                                              accept_callback))
        self._pool.putRequest(req)
        # threadpool also has callback support,
        # but for some reason the callback is not triggered
        # before you've collected the results.
        # Clear the results (if any), so it doesn't grow too large.
        self._pool._results_queue.queue.clear()
        return req
