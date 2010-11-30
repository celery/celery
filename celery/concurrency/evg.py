from gevent import Greenlet
from gevent.pool import Pool

from celery.concurrency.base import apply_target, BasePool


class TaskPool(BasePool):
    Pool = Pool

    signal_safe = False

    def on_start(self):
        self._pool = self.Pool(self.limit)

    def on_stop(self):
        if self._pool is not None:
            self._pool.join()

    def on_apply(self, target, args=None, kwargs=None, callback=None,
            accept_callback=None, **_):
        return self._pool.spawn(apply_target, target, args, kwargs,
                                callback, accept_callback)

    def blocking(self, fun, *args, **kwargs):
        Greenlet.spawn(fun, *args, **kwargs).get()

    @classmethod
    def on_import(cls):
        from gevent import monkey
        monkey.patch_all()
TaskPool.on_import()
