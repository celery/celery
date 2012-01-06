# -*- coding: utf-8 -*-
"""
celery.contrib.batches
======================

Collect messages and processes them as a list.

**Example**

A click counter that flushes the buffer every 100 messages, and every
10 seconds.

.. code-block:: python

    from celery.task import task
    from celery.contrib.batches import Batches

    # Flush after 100 messages, or 10 seconds.
    @task(base=Batches, flush_every=100, flush_interval=10)
    def count_click(requests):
        from collections import Counter
        count = Counter(request.kwargs["url"] for request in requests)
        for url, count in count.items():
            print(">>> Clicks: %s -> %s" % (url, count))

Registering the click is done as follows:

    >>> count_click.delay(url="http://example.com")

.. warning::

    For this to work you have to set
    :setting:`CELERYD_PREFETCH_MULTIPLIER` to zero, or some value where
    the final multiplied value is higher than ``flush_every``.

    In the future we hope to add the ability to direct batching tasks
    to a channel with different QoS requirements than the task channel.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from itertools import count
from Queue import Empty, Queue

from celery.task import Task
from celery.utils import cached_property, timer2
from celery.worker import state


def consume_queue(queue):
    """Iterator yielding all immediately available items in a
    :class:`Queue.Queue`.

    The iterator stops as soon as the queue raises :exc:`Queue.Empty`.

    *Examples*

        >>> q = Queue()
        >>> map(q.put, range(4))
        >>> list(consume_queue(q))
        [0, 1, 2, 3]
        >>> list(consume_queue(q))
        []

    """
    get = queue.get_nowait
    while 1:
        try:
            yield get()
        except Empty:
            break


def apply_batches_task(task, args, loglevel, logfile):
    task.request.update({"loglevel": loglevel, "logfile": logfile})
    try:
        result = task(*args)
    except Exception, exp:
        result = None
        task.logger.error("There was an Exception: %s", exp, exc_info=True)
    finally:
        task.request.clear()
    return result


class SimpleRequest(object):
    """Pickleable request."""

    #: task id
    id = None

    #: task name
    name = None

    #: positional arguments
    args = ()

    #: keyword arguments
    kwargs = {}

    #: message delivery information.
    delivery_info = None

    #: worker node name
    hostname = None

    def __init__(self, id, name, args, kwargs, delivery_info, hostname):
        self.id = id
        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.delivery_info = delivery_info
        self.hostname = hostname

    @classmethod
    def from_request(cls, request):
        return cls(request.task_id, request.task_name, request.args,
                   request.kwargs, request.delivery_info, request.hostname)


class Batches(Task):
    abstract = True

    #: Maximum number of message in buffer.
    flush_every = 10

    #: Timeout in seconds before buffer is flushed anyway.
    flush_interval = 30

    def __init__(self):
        self._buffer = Queue()
        self._count = count(1).next
        self._tref = None
        self._pool = None
        self._logging = None

    def run(self, requests):
        raise NotImplementedError("%r must implement run(requests)" % (self, ))

    def flush(self, requests):
        return self.apply_buffer(requests, ([SimpleRequest.from_request(r)
                                                for r in requests], ))

    def execute(self, request, pool, loglevel, logfile):
        if not self._pool:         # just take pool from first task.
            self._pool = pool
        if not self._logging:
            self._logging = loglevel, logfile

        state.task_ready(request)  # immediately remove from worker state.
        self._buffer.put(request)

        if self._tref is None:     # first request starts flush timer.
            self._tref = timer2.apply_interval(self.flush_interval * 1000,
                                               self._do_flush)

        if not self._count() % self.flush_every:
            self._do_flush()

    def _do_flush(self):
        self.debug("Wake-up to flush buffer...")
        requests = None
        if self._buffer.qsize():
            requests = list(consume_queue(self._buffer))
            if requests:
                self.debug("Buffer complete: %s" % (len(requests), ))
                self.flush(requests)
        if not requests:
            self.debug("Cancelling timer: Nothing in buffer.")
            self._tref.cancel()  # cancel timer.
            self._tref = None

    def apply_buffer(self, requests, args=(), kwargs={}):
        acks_late = [], []
        [acks_late[r.task.acks_late].append(r) for r in requests]
        assert requests and (acks_late[True] or acks_late[False])

        def on_accepted(pid, time_accepted):
            [req.acknowledge() for req in acks_late[False]]

        def on_return(result):
            [req.acknowledge() for req in acks_late[True]]

        loglevel, logfile = self._logging
        return self._pool.apply_async(apply_batches_task,
                    (self, args, loglevel, logfile),
                    accept_callback=on_accepted,
                    callback=acks_late[True] and on_return or None)

    def debug(self, msg):
        self.logger.debug("%s: %s", self.name, msg)

    @cached_property
    def logger(self):
        return self.app.log.get_default_logger()
