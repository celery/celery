from UserList import UserList


class PositionQueue(UserList):
    """A positional queue with filled/unfilled slots."""

    class UnfilledPosition(object):
        """Describes an unfilled slot."""
        def __init__(self, position):
            self.position = position

    def __init__(self, length):
        """Initialize a position queue with ``length`` slots."""
        self.length = length
        self.data = map(self.UnfilledPosition, xrange(length))

    def is_full(self):
        """Returns ``True`` if all the positions has been filled."""
        return len(self) >= self.length

    def __len__(self):
        """len(self) -> number of positions filled with real values."""
        return len(self.filled)

    @property
    def filled(self):
        """Returns the filled slots as a list."""
        return filter(lambda v: not isinstance(v, self.UnfilledPosition),
                      self)

        
