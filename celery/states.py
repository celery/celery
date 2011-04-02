"""
celery.states
=============

Built-in Task States.

:copyright: (c) 2009 - 2011 by Ask Solem.
:license: BSD, see LICENSE for more details.


.. _states:

States
------

See :ref:`task-states`.


Sets
----

.. state:: READY_STATES

READY_STATES
~~~~~~~~~~~~

Set of states meaning the task result is ready (has been executed).

.. state:: UNREADY_STATES

UNREADY_STATES
~~~~~~~~~~~~~~

Set of states meaning the task result is not ready (has not been executed).

.. state:: EXCEPTION_STATES

EXCEPTION_STATES
~~~~~~~~~~~~~~~~

Set of states meaning the task returned an exception.

.. state:: PROPAGATE_STATES

PROPAGATE_STATES
~~~~~~~~~~~~~~~~

Set of exception states that should propagate exceptions to the user.

.. state:: ALL_STATES

ALL_STATES
~~~~~~~~~~

Set of all possible states.


Misc.
-----

"""

#: State precedence.
#: None represents the precedence of an unknown state.
#: Lower index means higher precedence.
PRECEDENCE = ["SUCCESS",
              "FAILURE",
              None,
              "REVOKED",
              "STARTED",
              "RECEIVED",
              "RETRY",
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

PENDING = "PENDING"
RECEIVED = "RECEIVED"
STARTED = "STARTED"
SUCCESS = "SUCCESS"
FAILURE = "FAILURE"
REVOKED = "REVOKED"
RETRY = "RETRY"

READY_STATES = frozenset([SUCCESS, FAILURE, REVOKED])
UNREADY_STATES = frozenset([PENDING, RECEIVED, STARTED, RETRY])
EXCEPTION_STATES = frozenset([RETRY, FAILURE, REVOKED])
PROPAGATE_STATES = frozenset([FAILURE, REVOKED])

ALL_STATES = frozenset([PENDING, RECEIVED, STARTED,
                        SUCCESS, FAILURE, RETRY, REVOKED])
