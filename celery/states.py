""" Task States

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

"""
PENDING = "PENDING"
STARTED = "STARTED"
SUCCESS = "SUCCESS"
FAILURE = "FAILURE"
REVOKED = "REVOKED"
RETRY = "RETRY"


"""
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
READY_STATES = frozenset([SUCCESS, FAILURE, REVOKED])
UNREADY_STATES = frozenset([PENDING, STARTED, RETRY])
EXCEPTION_STATES = frozenset([RETRY, FAILURE, REVOKED])
PROPAGATE_STATES = frozenset([FAILURE, REVOKED])

ALL_STATES = frozenset([PENDING, STARTED, SUCCESS, FAILURE, RETRY, REVOKED])
