from time import sleep

from celery import Task, chain, chord, group, shared_task
from celery.exceptions import SoftTimeLimitExceeded
from celery.utils.log import get_task_logger

from .conftest import get_redis_connection

logger = get_task_logger(__name__)


@shared_task
def identity(x):
    """Return the argument."""
    return x


@shared_task
def add(x, y):
    """Add two numbers."""
    return x + y


@shared_task
def raise_error():
    """Deliberately raise an error."""
    raise ValueError("deliberate error")


@shared_task(ignore_result=True)
def add_ignore_result(x, y):
    """Add two numbers."""
    return x + y


@shared_task
def chain_add(x, y):
    (
        add.s(x, x) | add.s(y)
    ).apply_async()


@shared_task
def chord_add(x, y):
    chord(add.s(x, x), add.s(y)).apply_async()


@shared_task
def delayed_sum(numbers, pause_time=1):
    """Sum the iterable of numbers."""
    # Allow the task to be in STARTED state for
    # a limited period of time.
    sleep(pause_time)
    return sum(numbers)


@shared_task
def delayed_sum_with_soft_guard(numbers, pause_time=1):
    """Sum the iterable of numbers."""
    try:
        sleep(pause_time)
        return sum(numbers)
    except SoftTimeLimitExceeded:
        return 0


@shared_task
def tsum(nums):
    """Sum an iterable of numbers."""
    return sum(nums)


@shared_task(bind=True)
def add_replaced(self, x, y):
    """Add two numbers (via the add task)."""
    raise self.replace(add.s(x, y))


@shared_task(bind=True)
def add_to_all(self, nums, val):
    """Add the given value to all supplied numbers."""
    subtasks = [add.s(num, val) for num in nums]
    raise self.replace(group(*subtasks))


@shared_task(bind=True)
def add_to_all_to_chord(self, nums, val):
    for num in nums:
        self.add_to_chord(add.s(num, val))
    return 0


@shared_task(bind=True)
def add_chord_to_chord(self, nums, val):
    subtasks = [add.s(num, val) for num in nums]
    self.add_to_chord(group(subtasks) | tsum.s())
    return 0


@shared_task
def print_unicode(log_message='håå®ƒ valmuefrø', print_message='hiöäüß'):
    """Task that both logs and print strings containing funny characters."""
    logger.warning(log_message)
    print(print_message)


@shared_task
def return_exception(e):
    """Return a tuple containing the exception message and sentinel value."""
    return e, True


@shared_task
def sleeping(i, **_):
    """Task sleeping for ``i`` seconds, and returning nothing."""
    sleep(i)


@shared_task(bind=True)
def ids(self, i):
    """Returns a tuple of ``root_id``, ``parent_id`` and
    the argument passed as ``i``."""
    return self.request.root_id, self.request.parent_id, i


@shared_task(bind=True)
def collect_ids(self, res, i):
    """Used as a callback in a chain or group where the previous tasks
    are :task:`ids`: returns a tuple of::

        (previous_result, (root_id, parent_id, i))
    """
    return res, (self.request.root_id, self.request.parent_id, i)


@shared_task(bind=True, expires=60.0, max_retries=1)
def retry_once(self, *args, expires=60.0, max_retries=1, countdown=0.1):
    """Task that fails and is retried. Returns the number of retries."""
    if self.request.retries:
        return self.request.retries
    raise self.retry(countdown=countdown,
                     max_retries=max_retries)


@shared_task(bind=True, expires=60.0, max_retries=1)
def retry_once_priority(self, *args, expires=60.0, max_retries=1, countdown=0.1):
    """Task that fails and is retried. Returns the priority."""
    if self.request.retries:
        return self.request.delivery_info['priority']
    raise self.retry(countdown=countdown,
                     max_retries=max_retries)


@shared_task
def redis_echo(message):
    """Task that appends the message to a redis list."""
    redis_connection = get_redis_connection()
    redis_connection.rpush('redis-echo', message)


@shared_task(bind=True)
def second_order_replace1(self, state=False):

    redis_connection = get_redis_connection()
    if not state:
        redis_connection.rpush('redis-echo', 'In A')
        new_task = chain(second_order_replace2.s(),
                         second_order_replace1.si(state=True))
        raise self.replace(new_task)
    else:
        redis_connection.rpush('redis-echo', 'Out A')


@shared_task(bind=True)
def second_order_replace2(self, state=False):
    redis_connection = get_redis_connection()
    if not state:
        redis_connection.rpush('redis-echo', 'In B')
        new_task = chain(redis_echo.s("In/Out C"),
                         second_order_replace2.si(state=True))
        raise self.replace(new_task)
    else:
        redis_connection.rpush('redis-echo', 'Out B')


@shared_task(bind=True)
def build_chain_inside_task(self):
    """Task to build a chain.

    This task builds a chain and returns the chain's AsyncResult
    to verify that Asyncresults are correctly converted into
    serializable objects"""
    test_chain = (
        add.s(1, 1) |
        add.s(2) |
        group(
            add.s(3),
            add.s(4)
        ) |
        add.s(5)
    )
    result = test_chain()
    return result


class ExpectedException(Exception):
    """Sentinel exception for tests."""

    def __eq__(self, other):
        return (
            other is not None and
            isinstance(other, ExpectedException) and
            self.args == other.args
        )

    def __hash__(self):
        return hash(self.args)


@shared_task
def fail(*args):
    """Task that simply raises ExpectedException."""
    args = ("Task expected to fail",) + args
    raise ExpectedException(*args)


@shared_task
def chord_error(*args):
    return args


@shared_task(bind=True)
def return_priority(self, *_args):
    return "Priority: %s" % self.request.delivery_info['priority']


class ClassBasedAutoRetryTask(Task):
    name = 'auto_retry_class_task'
    autoretry_for = (ValueError,)
    retry_kwargs = {'max_retries': 1}
    retry_backoff = True

    def run(self):
        if self.request.retries:
            return self.request.retries
        raise ValueError()
