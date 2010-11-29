from eventlet import GreenPile, GreenPool
from eventlet import hubs
from eventlet import spawn
from eventlet.queue import LightQueue

from celery.concurrency.base import apply_target, BasePool


class TaskPool(BasePool):

    def _forever_wait_for_pile(self):
        avail = self._avail
        pile = self._pile

        while self.active:
            try:
                avail.queue.clear()
                pile.next()
            except StopIteration:
                avail.get(block=True)  # wait for task

    def on_start(self):
        hubs.use_hub()
        self._avail = LightQueue()
        self._pool = GreenPool(self.limit)
        self._pile = GreenPile(self._pool)

        spawn(self._forever_wait_for_pile)

    def on_stop(self):
        if self._pool is not None:
            self._pool.waitall()

    def on_apply(self, target, args=None, kwargs=None, callback=None,
            accept_callback=None, **_):
        self._pile.spawn(apply_target, target, args, kwargs,
                         callback, accept_callback)
        self._avail.put(1)  # notify waiters of new tasks.

    @classmethod
    def on_import(cls):
        import eventlet
        eventlet.monkey_patch()
TaskPool.on_import()
