"""

Custom Datastructures

"""
from UserList import UserList
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
