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

"""
PENDING = "PENDING"
STARTED = "STARTED"
SUCCESS = "SUCCESS"
FAILURE = "FAILURE"
RETRY = "RETRY"


"""
.. data:: READY_STATES

    Set of states meaning the task result is ready (has been executed).

.. data:: UNREADY_STATES

    Set of states meaning the task result is not ready (has not been executed).

.. data:: EXCEPTION_STATES

    Set of states meaning the task returned an exception.

.. data:: ALL_STATES

    Set of all possible states.

"""
READY_STATES = frozenset([SUCCESS, FAILURE])
UNREADY_STATES = frozenset([PENDING, STARTED, RETRY])
EXCEPTION_STATES = frozenset([RETRY, FAILURE])

ALL_STATES = frozenset([PENDING, STARTED, SUCCESS, FAILURE, RETRY])
