import os

if not os.environ.get("GEVENT_NOPATCH"):
    from gevent import monkey
    monkey.patch_all()

from celery.concurrency.base import apply_target, BasePool


class TaskPool(BasePool):
    signal_safe = False
    is_green = True

    def __init__(self, *args, **kwargs):
        from gevent import spawn_raw
        from gevent.pool import Pool
        self.Pool = Pool
        self.spawn_n = spawn_raw

    def on_start(self):
        self._pool = self.Pool(self.limit)

    def on_stop(self):
        if self._pool is not None:
            self._pool.join()

    def on_apply(self, target, args=None, kwargs=None, callback=None,
            accept_callback=None, **_):
        return self._pool.spawn(apply_target, target, args, kwargs,
                                callback, accept_callback)
