import threading

from collections import deque
from time import time, sleep
from Queue import Queue, Empty

from celery.datastructures import TokenBucket
from celery.utils import timeutils
from celery.utils.compat import all, izip_longest, chain_from_iterable


class RateLimitExceeded(Exception):
    """The token buckets rate limit has been exceeded."""


class TaskBucket(object):
    """This is a collection of token buckets, each task type having
    its own token bucket.  If the task type doesn't have a rate limit,
    it will have a plain :class:`~Queue.Queue` object instead of a
    :class:`TokenBucketQueue`.

    The :meth:`put` operation forwards the task to its appropriate bucket,
    while the :meth:`get` operation iterates over the buckets and retrieves
    the first available item.

    Say we have three types of tasks in the registry: `celery.ping`,
    `feed.refresh` and `video.compress`, the TaskBucket will consist
    of the following items::

        {"celery.ping": TokenBucketQueue(fill_rate=300),
         "feed.refresh": Queue(),
         "video.compress": TokenBucketQueue(fill_rate=2)}

    The get operation will iterate over these until one of the buckets
    is able to return an item.  The underlying datastructure is a `dict`,
    so the order is ignored here.

    :param task_registry: The task registry used to get the task
                          type class for a given task name.

    """

    def __init__(self, task_registry):
        self.task_registry = task_registry
        self.buckets = {}
        self.init_with_registry()
        self.immediate = deque()
        self.mutex = threading.Lock()
        self.not_empty = threading.Condition(self.mutex)

    def put(self, request):
        """Put a :class:`~celery.worker.job.TaskRequest` into
        the appropiate bucket."""
        self.mutex.acquire()
        try:
            if request.task_name not in self.buckets:
                self.add_bucket_for_type(request.task_name)
            self.buckets[request.task_name].put_nowait(request)
            self.not_empty.notify()
        finally:
            self.mutex.release()
    put_nowait = put

    def _get_immediate(self):
        try:
            return self.immediate.popleft()
        except IndexError:
            raise Empty()

    def _get(self):
        # If the first bucket is always returning items, we would never
        # get to fetch items from the other buckets. So we always iterate over
        # all the buckets and put any ready items into a queue called
        # "immediate". This queue is always checked for cached items first.
        try:
            return 0, self._get_immediate()
        except Empty:
            pass

        remaining_times = []
        for bucket in self.buckets.values():
            remaining = bucket.expected_time()
            if not remaining:
                try:
                    # Just put any ready items into the immediate queue.
                    self.immediate.append(bucket.get_nowait())
                except Empty:
                    pass
                except RateLimitExceeded:
                    remaining_times.append(bucket.expected_time())
            else:
                remaining_times.append(remaining)

        # Try the immediate queue again.
        try:
            return 0, self._get_immediate()
        except Empty:
            if not remaining_times:
                # No items in any of the buckets.
                raise

            # There's items, but have to wait before we can retrieve them,
            # return the shortest remaining time.
            return min(remaining_times), None

    def get(self, block=True, timeout=None):
        """Retrive the task from the first available bucket.

        Available as in, there is an item in the queue and you can
        consume tokens from it.

        """
        time_start = time()
        did_timeout = lambda: timeout and time() - time_start > timeout

        self.not_empty.acquire()
        try:
            while True:
                try:
                    remaining_time, item = self._get()
                except Empty:
                    if not block or did_timeout():
                        raise
                    self.not_empty.wait(timeout)
                    continue
                if remaining_time:
                    if not block or did_timeout():
                        raise Empty()
                    sleep(min(remaining_time, timeout or 1))
                else:
                    return item
        finally:
            self.not_empty.release()

    def get_nowait(self):
        return self.get(block=False)

    def init_with_registry(self):
        """Initialize with buckets for all the task types in the registry."""
        for task in self.task_registry.keys():
            self.add_bucket_for_type(task)

    def refresh(self):
        """Refresh rate limits for all task types in the registry."""
        for task in self.task_registry.keys():
            self.update_bucket_for_type(task)

    def get_bucket_for_type(self, task_name):
        """Get the bucket for a particular task type."""
        if task_name not in self.buckets:
            return self.add_bucket_for_type(task_name)
        return self.buckets[task_name]

    def _get_queue_for_type(self, task_name):
        bucket = self.buckets[task_name]
        if isinstance(bucket, TokenBucketQueue):
            return bucket.queue
        return bucket

    def update_bucket_for_type(self, task_name):
        task_type = self.task_registry[task_name]
        rate_limit = getattr(task_type, "rate_limit", None)
        rate_limit = timeutils.rate(rate_limit)
        task_queue = FastQueue()
        if task_name in self.buckets:
            task_queue = self._get_queue_for_type(task_name)
        else:
            task_queue = FastQueue()

        if rate_limit:
            task_queue = TokenBucketQueue(rate_limit, queue=task_queue)

        self.buckets[task_name] = task_queue
        return task_queue

    def add_bucket_for_type(self, task_name):
        """Add a bucket for a task type.

        Will read the tasks rate limit and create a :class:`TokenBucketQueue`
        if it has one.  If the task doesn't have a rate limit
        :class:`FastQueue` will be used instead.

        """
        if task_name not in self.buckets:
            return self.update_bucket_for_type(task_name)

    def qsize(self):
        """Get the total size of all the queues."""
        return sum(bucket.qsize() for bucket in self.buckets.values())

    def empty(self):
        """Returns :const:`True` if all of the buckets are empty."""
        return all(bucket.empty() for bucket in self.buckets.values())

    def clear(self):
        """Delete the data in all of the buckets."""
        for bucket in self.buckets.values():
            bucket.clear()

    @property
    def items(self):
        """Flattens the data in all of the buckets into a single list."""
        # for queues with contents [(1, 2), (3, 4), (5, 6), (7, 8)]
        # zips and flattens to [1, 3, 5, 7, 2, 4, 6, 8]
        return filter(None, chain_from_iterable(izip_longest(*[bucket.items
                                    for bucket in self.buckets.values()])))


class FastQueue(Queue):
    """:class:`Queue.Queue` supporting the interface of
    :class:`TokenBucketQueue`."""

    def clear(self):
        return self.queue.clear()

    def expected_time(self, tokens=1):
        return 0

    def wait(self, block=True):
        return self.get(block=block)

    @property
    def items(self):
        return self.queue


class TokenBucketQueue(object):
    """Queue with rate limited get operations.

    This uses the token bucket algorithm to rate limit the queue on get
    operations.

    :param fill_rate: The rate in tokens/second that the bucket will
                      be refilled.
    :keyword capacity: Maximum number of tokens in the bucket.
                       Default is 1.

    """
    RateLimitExceeded = RateLimitExceeded

    def __init__(self, fill_rate, queue=None, capacity=1):
        self._bucket = TokenBucket(fill_rate, capacity)
        self.queue = queue
        if not self.queue:
            self.queue = Queue()

    def put(self, item, block=True):
        """Put an item onto the queue."""
        self.queue.put(item, block=block)

    def put_nowait(self, item):
        """Put an item into the queue without blocking.

        :raises Queue.Full: If a free slot is not immediately available.

        """
        return self.put(item, block=False)

    def get(self, block=True):
        """Remove and return an item from the queue.

        :raises RateLimitExceeded: If a token could not be consumed from the
                                   token bucket (consuming from the queue
                                   too fast).
        :raises Queue.Empty: If an item is not immediately available.

        """
        get = block and self.queue.get or self.queue.get_nowait

        if not block and not self.items:
            raise Empty()

        if not self._bucket.can_consume(1):
            raise RateLimitExceeded()

        return get()

    def get_nowait(self):
        """Remove and return an item from the queue without blocking.

        :raises RateLimitExceeded: If a token could not be consumed from the
                                   token bucket (consuming from the queue
                                   too fast).
        :raises Queue.Empty: If an item is not immediately available.

        """
        return self.get(block=False)

    def qsize(self):
        """Returns the size of the queue."""
        return self.queue.qsize()

    def empty(self):
        """Returns :const:`True` if the queue is empty."""
        return self.queue.empty()

    def clear(self):
        """Delete all data in the queue."""
        return self.items.clear()

    def wait(self, block=False):
        """Wait until a token can be retrieved from the bucket and return
        the next item."""
        while True:
            remaining = self.expected_time()
            if not remaining:
                return self.get(block=block)
            sleep(remaining)

    def expected_time(self, tokens=1):
        """Returns the expected time in seconds of when a new token should be
        available."""
        if not self.items:
            return 0
        return self._bucket.expected_time(tokens)

    @property
    def items(self):
        """Underlying data.  Do not modify."""
        return self.queue.queue
