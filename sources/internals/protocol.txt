=======================
 Task Message Protocol
=======================

    * task
        ``string``

        Name of the task. **required**

    * id
        ``string``

        Unique id of the task (UUID). **required**

    * args
        ``list``

        List of arguments. Will be an empty list if not provided.

    * kwargs
        ``dictionary``

        Dictionary of keyword arguments. Will be an empty dictionary if not
        provided.

    * retries
        ``int``

        Current number of times this task has been retried.
        Defaults to ``0`` if not specified.

    * eta
        ``string`` (ISO 8601)

        Estimated time of arrival. This is the date and time in ISO 8601
        format. If not provided the message is not scheduled, but will be
        executed asap.

Example
=======

This is an example invocation of the ``celery.task.PingTask`` task in JSON
format::

    {"task": "celery.task.PingTask",
     "args": [],
     "kwargs": {},
     "retries": 0,
     "eta": "2009-11-17T12:30:56.527191"}


Serialization
=============

The protocol supports several serialization formats using the
``content_type`` message header.

The MIME-types supported by default are shown in the following table.

    =============== =================================
         Scheme                 MIME Type
    =============== =================================
    json            application/json
    yaml            application/x-yaml
    pickle          application/x-python-serialize
    =============== =================================
