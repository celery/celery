import heapq
import time


class Scheduler(object):

    def __init__(self, bucket_queue):
        self.bucket_queue = bucket_queue
        self._queue = []

    def enter(self, item, eta=None, priority=0, callback=None):
        eta = time.mktime(eta.timetuple()) if eta else time.time()
        heapq.heappush(self._queue, (eta, priority, item, callback))

    def __iter__(self):
        """The iterator yields the time to sleep for between runs."""

        # localize variable access
        q = self._queue
        nowfun = time.time
        pop = heapq.heappop
        bucket = self.bucket_queue

        while True:
            if q:
                eta, priority, item, callback = verify = q[0]
                now = nowfun()

                if now < eta:
                    yield eta - now
                else:
                    event = pop(q)
                    print("eta->%s priority->%s item->%s" % (
                        eta, priority, item))

                    if event is verify:
                        bucket.put(item)
                        callback and callback()
                        yield 0
                    else:
                        heapq.heappush(q, event)
            yield 1

    def empty(self):
        return not self._queue

    @property
    def queue(self):
        events = list(self._queue)
        return map(heapq.heappop, [events]*len(events))
