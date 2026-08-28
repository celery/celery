"""Example of bulk task producing with eventlet.

This module shows how to submit many tasks concurrently from a single
process without opening a new broker connection for every call to
``apply_async``.

``ProducerPool`` keeps a pool of eventlet greenlets ("producers"),
each of which acquires its own producer/connection once and reuses it
for every task it handles, rather than opening a new connection per
task. Callers push
``(task, args, kwargs, options)`` tuples onto an internal queue via
:meth:`ProducerPool.apply_async`, and whichever greenlet is free next
picks up the job and dispatches it. This is useful when you need to
publish a large, bursty batch of tasks from client code (e.g. a web
request handler) as fast as possible, since only a small, fixed
number of connections (one per greenlet) are opened for the whole
batch instead of one per task.

Usage::

    >>> app = Celery(broker='amqp://')
    >>> pool = ProducerPool(app, size=20)
    >>> receipt = pool.apply_async(some_task, (1, 2), {})
    >>> receipt.wait()  # block until the task has been published
    >>> result = receipt.result  # the AsyncResult from task.apply_async

"""
from eventlet import Timeout, monkey_patch, spawn_n
from eventlet.event import Event
from eventlet.queue import LightQueue

monkey_patch()


class Receipt:
    result = None

    def __init__(self, callback=None):
        self.callback = callback
        self.ready = Event()

    def finished(self, result):
        self.result = result
        if self.callback:
            self.callback(result)
        self.ready.send()

    def wait(self, timeout=None):
        with Timeout(timeout):
            return self.ready.wait()


class ProducerPool:
    """Usage::

        >>> app = Celery(broker='amqp://')
        >>> ProducerPool(app)

    """
    Receipt = Receipt

    def __init__(self, app, size=20):
        self.app = app
        self.size = size
        self.inqueue = LightQueue()
        self._running = None
        self._producers = None

    def apply_async(self, task, args, kwargs, callback=None, **options):
        if self._running is None:
            self._running = spawn_n(self._run)
        receipt = self.Receipt(callback)
        self.inqueue.put((task, args, kwargs, options, receipt))
        return receipt

    def _run(self):
        self._producers = [
            spawn_n(self._producer) for _ in range(self.size)
        ]

    def _producer(self):
        inqueue = self.inqueue

        with self.app.producer_or_acquire() as producer:
            while 1:
                task, args, kwargs, options, receipt = inqueue.get()
                result = task.apply_async(args, kwargs,
                                          producer=producer,
                                          **options)
                receipt.finished(result)
