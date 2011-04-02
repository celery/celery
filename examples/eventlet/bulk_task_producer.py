from __future__ import with_statement

from eventlet import spawn_n, monkey_patch, Timeout
from eventlet.queue import LightQueue
from eventlet.event import Event
from celery.messaging import establish_connection, TaskPublisher

monkey_patch()


class Receipt(object):
    result = None

    def __init__(self, callback=None):
        self.callback = None
        self.event = Event()

    def finished(self, result):
        self.result = result
        if self.callback:
            self.callback(result)
        self.ready.send()

    def wait(self, timeout=None):
        with Timeout(timeout):
            return self.ready.wait()


class ProducerPool(object):

    def __init__(self, size=20):
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
        self._producers = [spawn_n(self._producer)
                                for _ in xrange(self.size)]

    def _producer(self):
        connection = establish_connection()
        publisher = TaskPublisher(connection)
        inqueue = self.inqueue

        while 1:
            task, args, kwargs, options, receipt = inqueue.get()
            result = task.apply_async(args, kwargs,
                                      publisher=publisher,
                                      **options)
            receipt.finished(result)
