from __future__ import absolute_import, unicode_literals

from eventlet import Timeout, monkey_patch, spawn_n
from eventlet.event import Event
from eventlet.queue import LightQueue

monkey_patch()


class Receipt(object):
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


class ProducerPool(object):
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
