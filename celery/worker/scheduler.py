from __future__ import generators

import time
import heapq

from celery.worker.revoke import revoked
from celery import log

DEFAULT_MAX_INTERVAL = 2


class Scheduler(object):
    """ETA scheduler.

    :param ready_queue: Queue to move items ready for processing.
    :keyword max_interval: Maximum sleep interval between iterations.
        Default is 2 seconds.

    """

    def __init__(self, ready_queue, logger=None,
            max_interval=DEFAULT_MAX_INTERVAL):
        self.max_interval = float(max_interval)
        self.ready_queue = ready_queue
        self.logger = logger or log.get_default_logger()
        self._queue = []

    def enter(self, item, eta=None, priority=0, callback=None):
        """Enter item into the scheduler.

        :param item: Item to enter.
        :param eta: Scheduled time as a :class:`datetime.datetime` object.
        :param priority: Unused.
        :param callback: Callback to call when the item is scheduled.
            This callback takes no arguments.

        """
        eta = eta and time.mktime(eta.timetuple()) or time.time()
        heapq.heappush(self._queue, (eta, priority, item, callback))

    def __iter__(self):
        """The iterator yields the time to sleep for between runs."""

        # localize variable access
        nowfun = time.time
        pop = heapq.heappop
        ready_queue = self.ready_queue

        while 1:
            if self._queue:
                eta, priority, item, callback = verify = self._queue[0]
                now = nowfun()

                # FIXME: Need a generic hook for this
                if item.task_id in revoked:
                    event = pop(self._queue)
                    if event is verify:
                        item.on_ack()
                        self.logger.warn(
                                "Mediator: Skipping revoked task: %s[%s]" % (
                                    item.task_name, item.task_id))
                    else:
                        heapq.heappush(self._queue, event)

                if now < eta:
                    yield min(eta - now, self.max_interval)
                else:
                    event = pop(self._queue)

                    if event is verify:
                        ready_queue.put(item)
                        callback and callback()
                        yield 0
                    else:
                        heapq.heappush(self._queue, event)
            yield None

    def empty(self):
        """Is the schedule empty?"""
        return not self._queue

    def clear(self):
        self._queue = []

    def info(self):
        return ({"eta": eta, "priority": priority, "item": item}
                    for eta, priority, item, _ in self.queue)
    @property
    def queue(self):
        events = list(self._queue)
        return map(heapq.heappop, [events]*len(events))
