===============================
Django Models - celery.models
===============================

.. data:: TASK_STATUS_PENDING

    The string status of a pending task.

.. data:: TASK_STATUS_RETRY
   
    The string status of a task which is to be retried.

.. data:: TASK_STATUS_FAILURE
   
    The string status of a failed task.

.. data:: TASK_STATUS_DONE
   
    The string status of a task that was successfully executed.

.. data:: TASK_STATUSES
   
    List of possible task statuses.

.. data:: TASK_STATUSES_CHOICES
   
    Django choice tuple of possible task statuses, for usage in model/form
    fields ``choices`` argument.

.. class:: TaskMeta
   
    Model for storing the result and status of a task.
    
    *Note* Only used if you're running the ``database`` backend.

    .. attribute:: task_id

        The unique task id.

    .. attribute:: status

        The current status for this task.

    .. attribute:: result
        
        The result after successful/failed execution. If the task failed,
        this contains the execption it raised.

    .. attribute:: date_done

        The date this task changed status.

.. class:: PeriodicTaskMeta
   
    Metadata model for periodic tasks.

    .. attribute:: name
       
        The name of this task, as registered in the task registry.

    .. attribute:: last_run_at

        The date this periodic task was last run. Used to find out
        when it should be run next.

    .. attribute:: total_run_count
       
        The number of times this periodic task has been run.

    .. attribute:: task
       
        The class/function for this task.

    .. method:: delay()
        
        Delay the execution of a periodic task, and increment its total
        run count.
