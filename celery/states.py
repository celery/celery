"""

States
------

.. data:: PENDING

    Task is waiting for execution or unknown.

.. data:: STARTED

    Task has been started.

.. data:: SUCCESS

    Task has been successfully executed.

.. data:: FAILURE

    Task execution resulted in failure.

.. data:: RETRY

    Task is being retried.

.. data:: REVOKED

    Task has been revoked.

Sets
----

.. data:: READY_STATES

    Set of states meaning the task result is ready (has been executed).

.. data:: UNREADY_STATES

    Set of states meaning the task result is not ready (has not been executed).

.. data:: EXCEPTION_STATES

    Set of states meaning the task returned an exception.

.. data:: PROPAGATE_STATES

    Set of exception states that should propagate exceptions to the user.

.. data:: ALL_STATES

    Set of all possible states.

"""

## State precedence.
# None represents the precedence of an unknown state.
# Lower index means higher precedence.
PRECEDENCE = ["SUCCESS",
              "FAILURE",
              None,
              "REVOKED",
              "STARTED",
              "RECEIVED",
              "PENDING"]


def precedence(state):
    """Get the precedence index for state.

    Lower index means higher precedence.

    """
    try:
        return PRECEDENCE.index(state)
    except ValueError:
        return PRECEDENCE.index(None)


class state(str):
    """State is a subclass of :class:`str`, implementing comparison
    methods adhering to state precedence rules."""

    def compare(self, other, fun, default=False):
        return fun(precedence(self), precedence(other))

    def __gt__(self, other):
        return self.compare(other, lambda a, b: a < b, True)

    def __ge__(self, other):
        return self.compare(other, lambda a, b: a <= b, True)

    def __lt__(self, other):
        return self.compare(other, lambda a, b: a > b, False)

    def __le__(self, other):
        return self.compare(other, lambda a, b: a >= b, False)

PENDING = state("PENDING")
RECEIVED = state("RECEIVED")
STARTED = state("STARTED")
SUCCESS = state("SUCCESS")
FAILURE = state("FAILURE")
REVOKED = state("REVOKED")
RETRY = state("RETRY")

READY_STATES = frozenset([SUCCESS, FAILURE, REVOKED])
UNREADY_STATES = frozenset([PENDING, RECEIVED, STARTED, RETRY])
EXCEPTION_STATES = frozenset([RETRY, FAILURE, REVOKED])
PROPAGATE_STATES = frozenset([FAILURE, REVOKED])

ALL_STATES = frozenset([PENDING, RECEIVED, STARTED,
                        SUCCESS, FAILURE, RETRY, REVOKED])
