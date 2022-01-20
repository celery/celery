import asyncio
import threading
from threading import Thread

import janus
from asyncio_pool import AioPool

from celery.concurrency.base import BasePool


async def test():
    await asyncio.sleep(0.5)
    print("TEST HERE PASSED!!!!")


class AsyncJob:
    def __init__(self, func, args=(), kwargs=None,
                 callback=None, error_callback=None, accept_callback=None,
                 timeout_callback=None):
        self.timeout_callback = timeout_callback
        self.accept_callback = accept_callback
        self.error_callback = error_callback
        self.callback = callback
        self.kwargs = kwargs
        self.args = args
        self.func = func


class TaskPool(BasePool):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._tasks_queue = None
        self._pool = None
        self._result_handles = []

        self._guest_event_loop_thread = Thread(
            target=asyncio.run,
            args=(self._guest_thread_main(),)
        )
        self._initialized_event = threading.Event()

    def on_apply(self, *args, **kwargs):
        self._tasks_queue.sync_q.put(test)

    def on_start(self):
        self._guest_event_loop_thread.start()
        if not self._initialized_event.wait(5):
            raise TimeoutError("Could not initialize event loop.\n"
                               "This shouldn't happen.")

    def on_stop(self):
        self._tasks_queue.close()

        self._guest_event_loop_thread.join()

    async def _guest_thread_main(self):
        # these can only be initialized once the event loop
        # is started.
        self._tasks_queue = janus.Queue()
        self._pool = AioPool(size=self.limit)
        self._initialized_event.set()

        while not self._tasks_queue.closed:
            queue = self._tasks_queue.async_q
            try:
                task = await asyncio.wait_for(queue.get(), timeout=1)
                result_handle = self._pool.spawn_n(task(), queue.task_done)
                self._result_handles.append(result_handle)
            except asyncio.TimeoutError:
                pass

        await self._pool.join()
