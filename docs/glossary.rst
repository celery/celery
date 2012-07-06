.. _glossary:

Glossary
========

.. glossary::
    :sorted:

    request
        Task messages are converted to *requests* within the worker.
        The request information is also available as the task's
        :term:`context` (the ``task.request`` attribute).

    calling
        Sends a task message so that the task function is
        :term:`executed <executing>` by a worker.

    executing
        Workers *execute* task :term:`requests <request>`.

    apply
        Originally a synonym to :term:`call <calling>` but used to signify
        that a function is executed by the current process.

    context
        The context of a task contains information like the id of the task,
        it's arguments and what queue it was delivered to.
        It can be accessed as the tasks ``request`` attribute.
        See :ref:`task-request-info`
