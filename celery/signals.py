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



"""

.. data:: worker_init

Triggered before the worker is started.

"""
worker_init = Signal(providing_args=[])

"""

.. data:: worker_ready

Triggered when the worker is ready to accept work.

"""
worker_ready = Signal(providing_args=[])

"""

.. data:: worker_shutdown

Triggered when the worker is about to shut down.

"""
worker_shutdown = Signal(providing_args=[])
