.. _internals-task-message-protocol:

=======================
 Task Messages
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

* expires
    `string` (ISO 8601)
    .. versionadded:: 2.0.2

    Expiration date. This is the date and time in ISO 8601 format.
    If not provided the message will never expire. The message
    will be expired when the message is received and the expiration date
    has been exceeded.


Extensions
==========

Extensions are additional keys in the message body that the worker may or
may not support.  If the worker finds an extension key it doesn't support
it should optimally reject the message so another worker gets a chance
to process it.


* taskset
  `string`

  The taskset this task is part of.

* chord
  `object`
  .. versionadded:: 2.3

  Signifies that this task is one of the header parts of a chord.  The value
  of this key is the body of the cord that should be executed when all of
  the tasks in the header has returned.

* utc
  `bool`
  .. versionadded:: 2.5

  If true time uses the UTC timezone, if not the current local timezone
  should be used.

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

Several types of serialization formats are supported using the
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
