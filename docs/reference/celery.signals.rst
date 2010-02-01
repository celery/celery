========================================
Signals - celery.signals
========================================

.. data:: task_sent

    Triggered when a task has been sent to the broker.
    Note that this is executed in the client process, the one sending
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

.. data:: task_postrun

    Triggered after a task has been executed.

    Provides arguments:

    * task_id
        Id of the task to be executed.

    * task
        The task being executed.

    * args
        The tasks positional arguments.

    * kwargs
        The tasks keyword arguments.

    * retval

        The return value of the task.

.. data:: worker_init

    Triggered before the worker is started.

.. data:: worker_ready

    Triggered when the worker is ready to accept work.

.. data:: worker_shutdown

    Triggered when the worker is about to shut down.
