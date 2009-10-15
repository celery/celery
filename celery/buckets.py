import time
from Queue import Queue, Empty as QueueEmpty

RATE_MODIFIER_MAP = {"s": lambda n: n,
                     "m": lambda n: n / 60.0,
                     "h": lambda n: n / 60.0 / 60.0}


class BucketRateExceeded(Exception):
    """The token buckets rate limit has been exceeded."""


def parse_ratelimit_string(rate_limit):
    """Parse rate limit configurations such as ``"100/m"`` or ``"2/h"``
        and convert them into seconds."""
    if rate_limit:
        try:
            return int(rate_limit)
        except ValueError:
            ops, _, modifier = rate_limit.partition("/")
            return RATE_MODIFIER_MAP[modifier](int(ops))
    return None


class TaskBucket(object):
    """A bucket with buckets of tasks. (eh. seriously.)

    This is a collection of token buckets, each task type having
    its own token bucket. If the task type doesn't have a rate limit,
    it will have a plain Queue object instead of a token bucket queue.

    The :meth:`put` operation forwards the task to its appropriate bucket,
    while the :meth:`get` operation iterates over the buckets and retrieves
    the first available item.

    Say we have three types of tasks in the registry: ``celery.ping``,
    ``feed.refresh`` and ``video.compress``, the TaskBucket will consist
    of the following items::

        {"celery.ping": TokenBucketQueue(fill_rate=300),
         "feed.refresh": Queue(),
         "video.compress": TokenBucketQueue(fill_rate=2)}

    The get operation will iterate over these until one of them
    is able to return an item. The underlying datastructure is a ``dict``,
    so the order is ignored here.

    :param task_registry: The task registry used to get the task
        type class for a given task name.


    """
    min_wait = 0.05

    def __init__(self, task_registry):
        self.task_registry = task_registry
        self.buckets = {}
        self.init_with_registry()

    def put(self, task):
        """Put a task into the appropiate bucket."""
        self.buckets[task_name].put_nowait(task)

    def get(self, timeout=None):
        """Retrive the task from the first available bucket.

        Available as in, there is an item in the queue and you can
        consume tokens from it.

        """
        time_spent = 0
        for bucket in self.buckets.values():
            remaining_times = []
            try:
                return bucket.get_nowait()
            except BucketRateExceeded:
                remaining_times.append(bucket.expected_time())
            except QueueEmpty:
                pass

            if timeout and time_spent >= timeout:
                raise QueueEmpty()
            else:
                shortest_wait = min(remaining_times or [self.min_wait])
                time_spent += shortest_wait
                time.sleep(shortest_wait)

    def init_with_registry(self):
        """Initialize with buckets for all the task types in the registry."""
        map(self.add_bucket_for_type, self.task_registry.keys())

    def get_bucket_for_type(self, task_name):
        """Get the bucket for a particular task type."""
        if not task_name in self.buckets:
            return self.add_bucket_for_type(task_name)
        return self.buckets[task_name]

    def add_bucket_for_type(self, task_name):
        """Add a bucket for a task type.

        Will read the tasks rate limit and create a :class:`TokenBucketQueue`
        if it has one. If the task doesn't have a rate limit a regular Queue
        will be used.

        """
        task_type = self.task_registry[task_name]
        task_queue = Queue()
        rate_limit = getattr(task_type, "rate_limit", None)
        rate_limit = parse_ratelimit_string(rate_limit)
        if rate_limit:
            task_queue = TokenBucketQueue(rate_limit, queue=task_queue)

        self.buckets[task_name] = task_queue
        return task_queue


class TokenBucketQueue(object):
    """An implementation of the token bucket algorithm.

    See http://en.wikipedia.org/wiki/Token_Bucket
    Most of this code was stolen from an entry in the ASPN Python Cookbook:
    http://code.activestate.com/recipes/511490/

    :param fill_rate: see :attr:`fill_rate`.
    :keyword capacity: see :attr:`capacity`.

    .. attribute:: fill_rate

        The rate in tokens/second that the bucket will be refilled.

    .. attribute:: capacity

        Maximum number of tokens in the bucket. Default is ``1``.

    .. attribute:: timestamp

        Timestamp of the last time a token was taken out of the bucket.

    """
    def __init__(self, fill_rate, queue=None, capacity=1):
        self.capacity = float(capacity)
        self._tokens = self.capacity
        self.queue = queue
        if not self.queue:
            self.queue = Queue()
        self.fill_rate = float(fill_rate)
        self.timestamp = time.time()

    def put(self, item, nb=False):
        put = self.queue.put_nowait if nb else self.queue.put
        put(item)

    def get(self, nb=False):
        get = self.queue.get_nowait if nb else self.queue.get

        if not self.can_consume(1):
            raise BucketRateExceeded()

        return get()

    def get_nowait(self):
        return self.get(nb=True)

    def put_nowait(self, item):
        return self.put(item, nb=True)

    def qsize(self):
        return self.queue.qsize()

    def can_consume(self, tokens=1):
        """Consume tokens from the bucket. Returns True if there were
        sufficient tokens otherwise False."""
        if tokens <= self._get_tokens():
            self._tokens -= tokens
            return True
        return False

    def expected_time(self, tokens=1):
        """Returns the expected time in seconds when a new token should be
        available."""
        tokens = max(tokens, self._get_tokens())
        return (tokens - self._get_tokens()) / self.fill_rate

    def _get_tokens(self):
        if self._tokens < self.capacity:
            now = time.time()
            delta = self.fill_rate * (now - self.timestamp)
            self._tokens = min(self.capacity, self._tokens + delta)
            self.timestamp = now
        return self._tokens
