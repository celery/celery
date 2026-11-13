"""Module-level helpers for the subinterpreter pool tests.

Anything sent to a subinterpreter is pickled by reference, so it has to be
importable there: closures and test methods are not.
"""
import sys
from concurrent import interpreters

from celery import Celery

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
