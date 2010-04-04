""" Task States

.. data:: PENDING

    Task is waiting for execution or unknown.

.. data:: RUNNING

    Task is curretly executing on a worker

.. data:: SUCCESS

    Task has been successfully executed.

.. data:: FAILURE

    Task execution resulted in failure.

.. data:: RETRY

    Task is being retried.

"""
PENDING = "PENDING"
RUNNING = "RUNNING"
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
UNREADY_STATES = frozenset([PENDING, RUNNING, RETRY])
EXCEPTION_STATES = frozenset([RETRY, FAILURE])

ALL_STATES = frozenset([PENDING, RUNNING, SUCCESS, FAILURE, RETRY])


