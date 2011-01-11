.. _internals-task-message-protocol:

=======================
 Task Message Protocol
=======================

.. contents::
    :local:

Message format
==============

* task
    `string`

    Name of the task. **required**

* id
    `string`

    Unique id of the task (UUID). **required**

* args
    `list`

    List of arguments. Will be an empty list if not provided.

* kwargs
    `dictionary`

    Dictionary of keyword arguments. Will be an empty dictionary if not
    provided.

* retries
    `int`

    Current number of times this task has been retried.
    Defaults to `0` if not specified.

* eta
    `string` (ISO 8601)

    Estimated time of arrival. This is the date and time in ISO 8601
    format. If not provided the message is not scheduled, but will be
    executed asap.

* expires (introduced after v2.0.2)
    `string` (ISO 8601)

    Expiration date. This is the date and time in ISO 8601 format.
    If not provided the message will never expire. The message
    will be expired when the message is received and the expiration date
    has been exceeded.

Example message
===============

This is an example invocation of the `celery.task.PingTask` task in JSON
format:

.. code-block:: javascript

    {"id": "4cc7438e-afd4-4f8f-a2f3-f46567e7ca77",
     "task": "celery.task.PingTask",
     "args": [],
     "kwargs": {},
     "retries": 0,
     "eta": "2009-11-17T12:30:56.527191"}

Serialization
=============

The protocol supports several serialization formats using the
`content_type` message header.

The MIME-types supported by default are shown in the following table.

    =============== =================================
         Scheme                 MIME Type
    =============== =================================
    json            application/json
    yaml            application/x-yaml
    pickle          application/x-python-serialize
    msgpack         application/x-msgpack
    =============== =================================
