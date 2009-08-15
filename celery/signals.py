from django.dispatch import Signal


"""

.. DATA: task_prerun

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

.. DATA: task_postrun

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
