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

    def full(self):
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

        
class TaskProcessQueue(UserList):
    """Queue of running child processes, which starts waiting for the
    processes to finish when the queue limit is reached."""

    def __init__(self, limit, logger=None, done_msg=None):
        self.limit = limit
        self.logger = logger
        self.done_msg = done_msg
        self.data = []
        
    def add(self, result, task_name, task_id):
        self.data.append([result, task_name, task_id])

        if self.data and len(self.data) >= self.limit:
            for result, task_name, task_id in self.data:
                ret_value = result.get()
                if self.done_msg and self.logger:
                    self.logger.info(self.done_msg % {
                        "name": task_name,
                        "id": task_id,
                        "return_value": ret_value})
            self.data = []
