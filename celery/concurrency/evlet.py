
import threading

from eventlet import GreenPile, GreenPool
from eventlet import hubs
from eventlet import spawn_n
from eventlet.greenthread import sleep
from Queue import Empty, Queue as LightQueue

from celery import log
from celery.utils.functional import partial
from celery.datastructures import ExceptionInfo

RUN = 0x1
CLOSE = 0x2
TERMINATE = 0x3


accept_lock = threading.Lock()


def do_work(target, args=(), kwargs={}, callback=None,
        accept_callback=None, method_queue=None):
    method_queue.delegate(accept_callback)
    callback(target(*args, **kwargs))


class Waiter(threading.Thread):

    def __init__(self, inqueue, limit):
        self.inqueue = inqueue
        self.limit = limit
        self._state = None
        threading.Thread.__init__(self)

    def run(self):
        hubs.use_hub()
        pool = GreenPool(self.limit)
        pile = GreenPile(pool)
        self._state = RUN
        inqueue = self.inqueue

        def get_from_queue_forever():
            while self._state == RUN:
                try:
                    print("+INQUEUE GET")
                    m = inqueue.get_nowait()
                    print("-INQUEUE GET")
                except Empty:
                    sleep(0.3)
                else:
                    print("+SPAWN")
                    pile.spawn(*m)
                    print("-SPAWN")

        def wait_for_pile_forever():
            while self._state == RUN:
                print("+CALLING PILE NEXT")
                pile.next()
                print("-CALLING PILE NEXT")

        spawn_n(get_from_queue_forever)
        spawn_n(wait_for_pile_forever)

        while self._state == RUN:
            sleep(0.1)



class TaskPool(object):
    _state = None

    def __init__(self, limit, logger=None, **kwargs):
        self.limit = limit
        self.logger = logger or log.get_default_logger()
        self._pool = None

    def start(self):
        self._state = RUN
        self._out = LightQueue()
        self._waiter = Waiter(self._out, self.limit)
        self._waiter.start()

    def stop(self):
        self._state = CLOSE
        self._pool.pool.waitall()
        self._waiter._state = TERMINATE
        self._state = TERMINATE

    def apply_async(self, target, args=None, kwargs=None, callbacks=None,
            errbacks=None, accept_callback=None, **compat):
        if self._state != RUN:
            return
        args = args or []
        kwargs = kwargs or {}
        callbacks = callbacks or []
        errbacks = errbacks or []

        on_ready = partial(self.on_ready, callbacks, errbacks)

        self.logger.debug("GreenPile: Spawn %s (args:%s kwargs:%s)" % (
            target, args, kwargs))

        print("+OUTQUEUE.PUT")
        self._out.put((do_work, target, args, kwargs,
                      on_ready, accept_callback, self.method_queue))
        print("-OUTQUEUE.PUT")


    def on_ready(self, callbacks, errbacks, ret_value):
        """What to do when a worker task is ready and its return value has
        been collected."""

        if isinstance(ret_value, ExceptionInfo):
            if isinstance(ret_value.exception, (
                    SystemExit, KeyboardInterrupt)): # pragma: no cover
                raise ret_value.exception
            [errback(ret_value) for errback in errbacks]
        else:
            [callback(ret_value) for callback in callbacks]
