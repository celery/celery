import os

from gevent import monkey

if not os.environ.get("GEVENT_NOPATCH"):
    monkey.patch_all()

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
