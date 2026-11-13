"""Module-level helpers for the subinterpreter pool tests.

Anything sent to a subinterpreter is pickled by reference, so it has to be
importable there: closures and test methods are not.
"""
import sys
import time
from concurrent import interpreters

from celery import Celery
from celery.concurrency.subinterpreter import stop_requested

app = Celery('subinterpreter_helpers', set_as_current=False)
app.conf.include = [__name__]


@app.task
def add(x, y):
    return x + y


def current_interpreter_id():
    return interpreters.get_current().id


def get_sys_path():
    return list(sys.path)


def fail():
    raise ValueError('nope')


def fail_setup(*args):
    raise RuntimeError('cannot set up')


def fail_setup_in_some(*args):
    """Fail in interpreters with an even id: of 2+ created in a row, at
    least one fails and one succeeds."""
    if interpreters.get_current().id % 2 == 0:
        raise RuntimeError('cannot set up')


def is_stop_requested():
    return stop_requested()


def wait_for_stop(timeout):
    """Return whether the task was asked to stop within ``timeout``."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if stop_requested():
            return True
        time.sleep(0.01)
    return False
