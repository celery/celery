from eventlet import GreenPool
from eventlet import spawn

from celery.concurrency.base import apply_target, BasePool


class TaskPool(BasePool):
    Pool = GreenPool

    signal_safe = False

    def on_start(self):
        self._pool = self.Pool(self.limit)

    def on_stop(self):
        if self._pool is not None:
            self._pool.waitall()

    def on_apply(self, target, args=None, kwargs=None, callback=None,
            accept_callback=None, **_):
        self._pool.spawn(apply_target, target, args, kwargs,
                         callback, accept_callback)

    def blocking(self, fun, *args, **kwargs):
        return spawn(fun, *args, **kwargs).wait()

    @classmethod
    def on_import(cls):
        import eventlet
        import eventlet.debug
        eventlet.monkey_patch()
        eventlet.debug.hub_prevent_multiple_readers(False)
TaskPool.on_import()
