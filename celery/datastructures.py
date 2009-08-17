"""

Custom Datastructures

"""
from UserList import UserList
from Queue import Queue
from Queue import Empty as QueueEmpty
import traceback


class PositionQueue(UserList):
    """A positional queue of a specific length, with slots that are either
    filled or unfilled. When all of the positions are filled, the queue
    is considered :meth:`full`.

    :param length: see :attr:`length`.


    .. attribute:: length

        The number of items required for the queue to be considered full.

    """

    class UnfilledPosition(object):
        """Describes an unfilled slot."""

        def __init__(self, position):
            self.position = position

    def __init__(self, length):
        self.length = length
        self.data = map(self.UnfilledPosition, xrange(length))

    def full(self):
        """Returns ``True`` if all of the slots has been filled."""
        return len(self) >= self.length

    def __len__(self):
        """``len(self)`` -> number of slots filled with real values."""
        return len(self.filled)

    @property
    def filled(self):
        """Returns the filled slots as a list."""
        return filter(lambda v: not isinstance(v, self.UnfilledPosition),
                      self.data)


class ExceptionInfo(object):
    """Exception wrapping an exception and its traceback.

    :param exc_info: The exception tuple info as returned by
        :func:`traceback.format_exception`.


    .. attribute:: exception

        The original exception.

    .. attribute:: traceback

        A traceback from the point when :attr:`exception` was raised.

    """

    def __init__(self, exc_info):
        type_, exception, tb = exc_info
        self.exception = exception
        self.traceback = '\n'.join(traceback.format_exception(*exc_info))

    def __str__(self):
        return str(self.exception)


def consume_queue(queue):
    while True:
        try:
            yield queue.get_nowait()
        except QueueEmpty, exc:
            raise StopIteration()


class SharedCounter(object):
    """An integer that can be updated by several threads at once.

    Please note that the final value is not synchronized, this means
    that you should not update the value on a previous value, the only
    reliable operations are increment and decrement.

    Example

        >>> max_clients = SharedCounter(initial_value=10)

        # Thread one
        >>> max_clients += 1 # OK (safe)

        # Thread two
        >>> max_clients -= 3 # OK (safe)

        # Main thread
        >>> if client >= int(max_clients): # Max clients now at 8
        ...    wait()


        >>> max_client = max_clients + 10 # NOT OK (unsafe)

    """
    def __init__(self, initial_value):
        self._value = initial_value
        self._modify_queue = Queue()

    def increment(self):
        """Increment the value by one."""
        self += 1

    def decrement(self):
        """Decrement the value by one."""
        self -= 1
        
    def _update_value(self):
        self._value += sum(consume_queue(self._modify_queue))
        return self._value
    
    def __iadd__(self, y):
        """``self += y``"""
        self._modify_queue.put(y * +1)
        return self

    def __isub__(self, y):
        """``self -= y``"""
        self._modify_queue.put(y * -1)
        return self

    def __int__(self):
        """``int(self) -> int``"""
        self._update_value()
        return self._value

    def __repr__(self):
        return "<SharedCounter: int(%s)>" % str(int(self))
