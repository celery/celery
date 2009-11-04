from django.dispatch import Signal

"""

.. data:: task_sent

Triggered when a task has been sent to the broker.
Please note that this is executed in the client, the process sending
the task, not in the worker.

Provides arguments:

* task_id
    Id of the task to be executed.

* task
    The task being executed.

* args
    the tasks positional arguments.

* kwargs
    The tasks keyword arguments.

* eta
    The time to execute the task.

* taskset
    Id of the taskset this task is part of (if any).


"""
task_sent = Signal(providing_args=[
                        "task_id", "task", "args", "kwargs", "eta",
                        "taskset"])

"""
.. data:: task_prerun

Triggered before a task is executed.

Provides arguments:

* task_id
    Id of the task to be executed.

* task
    The task being executed.

* args
    the tasks positional arguments.

* kwargs
    The tasks keyword arguments.

"""
task_prerun = Signal(providing_args=[
                        "task_id", "task", "args", "kwargs"])

"""

.. data:: task_postrun

Triggered after a task has been executed.

Provides arguments:

* task_id
    Id of the task to be executed.

* task
    The task being executed.

* args
    the tasks positional arguments.

* kwargs
    The tasks keyword arguments.

* retval

    The return value of the task.

"""
task_postrun = Signal(providing_args=[
                        "task_id", "task", "args", "kwargs", "retval"])
