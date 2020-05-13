.. _message-protocol:

===================
 Message Protocol
===================

.. contents::
    :local:

.. _message-protocol-task:
.. _internals-task-message-protocol:

Task messages
=============

.. _message-protocol-task-v2:

Version 2
---------

Definition
~~~~~~~~~~

.. code-block:: python

    properties = {
        'correlation_id': uuid task_id,
        'content_type': string mimetype,
        'content_encoding': string encoding,

        # optional
        'reply_to': string queue_or_url,
    }
    headers = {
        'lang': string 'py'
        'task': string task,
        'id': uuid task_id,
        'root_id': uuid root_id,
        'parent_id': uuid parent_id,
        'group': uuid group_id,

        # optional
        'meth': string method_name,
        'shadow': string alias_name,
        'eta': iso8601 ETA,
        'expires': iso8601 expires,
        'retries': int retries,
        'timelimit': (soft, hard),
        'argsrepr': str repr(args),
        'kwargsrepr': str repr(kwargs),
        'origin': str nodename,
    }

    body = (
        object[] args,
        Mapping kwargs,
        Mapping embed {
            'callbacks': Signature[] callbacks,
            'errbacks': Signature[] errbacks,
            'chain': Signature[] chain,
            'chord': Signature chord_callback,
        }
    )

Example
~~~~~~~

This example sends a task message using version 2 of the protocol:

.. code-block:: python

    # chain: add(add(add(2, 2), 4), 8) == 2 + 2 + 4 + 8

    import json
    import os
    import socket

    task_id = uuid()
    args = (2, 2)
    kwargs = {}
    basic_publish(
        message=json.dumps((args, kwargs, None)),
        application_headers={
            'lang': 'py',
            'task': 'proj.tasks.add',
            'argsrepr': repr(args),
            'kwargsrepr': repr(kwargs),
            'origin': '@'.join([os.getpid(), socket.gethostname()])
        }
        properties={
            'correlation_id': task_id,
            'content_type': 'application/json',
            'content_encoding': 'utf-8',
        }
    )

Changes from version 1
~~~~~~~~~~~~~~~~~~~~~~

- Protocol version detected by the presence of a ``task`` message header.

- Support for multiple languages via the ``lang`` header.

    Worker may redirect the message to a worker that supports
    the language.

- Meta-data moved to headers.

    This means that workers/intermediates can inspect the message
    and make decisions based on the headers without decoding
    the payload (that may be language specific, for example serialized by the
    Python specific pickle serializer).

- Always UTC

    There's no ``utc`` flag anymore, so any time information missing timezone
    will be expected to be in UTC time.

- Body is only for language specific data.

    - Python stores args/kwargs and embedded signatures in body.

    - If a message uses raw encoding then the raw data
      will be passed as a single argument to the function.

    - Java/C, etc. can use a Thrift/protobuf document as the body

- ``origin`` is the name of the node sending the task.

- Dispatches to actor based on ``task``, ``meth`` headers

    ``meth`` is unused by Python, but may be used in the future
    to specify class+method pairs.

- Chain gains a dedicated field.

    Reducing the chain into a recursive ``callbacks`` argument
    causes problems when the recursion limit is exceeded.

    This is fixed in the new message protocol by specifying
    a list of signatures, each task will then pop a task off the list
    when sending the next message:

    .. code-block:: python

        execute_task(message)
        chain = embed['chain']
        if chain:
            sig = maybe_signature(chain.pop())
            sig.apply_async(chain=chain)

- ``correlation_id`` replaces ``task_id`` field.

- ``root_id`` and ``parent_id`` fields helps keep track of work-flows.

- ``shadow`` lets you specify a different name for logs, monitors
  can be used for concepts like tasks that calls a function
  specified as argument:

    .. code-block:: python

        from celery.utils.imports import qualname

        class PickleTask(Task):

            def unpack_args(self, fun, args=()):
                return fun, args

            def apply_async(self, args, kwargs, **options):
                fun, real_args = self.unpack_args(*args)
                return super(PickleTask, self).apply_async(
                    (fun, real_args, kwargs), shadow=qualname(fun), **options
                )

        @app.task(base=PickleTask)
        def call(fun, args, kwargs):
            return fun(*args, **kwargs)


.. _message-protocol-task-v1:
.. _task-message-protocol-v1:

Version 1
---------

In version 1 of the protocol all fields are stored in the message body:
meaning workers and intermediate consumers must deserialize the payload
to read the fields.

Message body
~~~~~~~~~~~~

* ``task``
    :`string`:

    Name of the task. **required**

* ``id``
    :`string`:

    Unique id of the task (UUID). **required**

* ``args``
    :`list`:

    List of arguments. Will be an empty list if not provided.

* ``kwargs``
    :`dictionary`:

    Dictionary of keyword arguments. Will be an empty dictionary if not
    provided.

* ``retries``
    :`int`:

    Current number of times this task has been retried.
    Defaults to `0` if not specified.

* ``eta``
    :`string` (ISO 8601):

    Estimated time of arrival. This is the date and time in ISO 8601
    format. If not provided the message isn't scheduled, but will be
    executed asap.

* ``expires``
    :`string` (ISO 8601):

    .. versionadded:: 2.0.2

    Expiration date. This is the date and time in ISO 8601 format.
    If not provided the message will never expire. The message
    will be expired when the message is received and the expiration date
    has been exceeded.

* ``taskset``
    :`string`:

    The group this task is part of (if any).

* ``chord``
    :`Signature`:

    .. versionadded:: 2.3

    Signifies that this task is one of the header parts of a chord. The value
    of this key is the body of the cord that should be executed when all of
    the tasks in the header has returned.

* ``utc``
    :`bool`:

    .. versionadded:: 2.5

    If true time uses the UTC timezone, if not the current local timezone
    should be used.

* ``callbacks``
    :`<list>Signature`:

    .. versionadded:: 3.0

    A list of signatures to call if the task exited successfully.

* ``errbacks``
    :`<list>Signature`:

    .. versionadded:: 3.0

    A list of signatures to call if an error occurs while executing the task.

* ``timelimit``
    :`<tuple>(float, float)`:

    .. versionadded:: 3.1

    Task execution time limit settings. This is a tuple of hard and soft time
    limit value (`int`/`float` or :const:`None` for no limit).

    Example value specifying a soft time limit of 3 seconds, and a hard time
    limit of 10 seconds::

        {'timelimit': (3.0, 10.0)}


Example message
~~~~~~~~~~~~~~~

This is an example invocation of a `celery.task.ping` task in json
format:

.. code-block:: javascript

    {"id": "4cc7438e-afd4-4f8f-a2f3-f46567e7ca77",
     "task": "celery.task.PingTask",
     "args": [],
     "kwargs": {},
     "retries": 0,
     "eta": "2009-11-17T12:30:56.527191"}

Task Serialization
------------------

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

.. _message-protocol-event:

Event Messages
==============

Event messages are always JSON serialized and can contain arbitrary message
body fields.

Since version 4.0. the body can consist of either a single mapping (one event),
or a list of mappings (multiple events).

There are also standard fields that must always be present in an event
message:

Standard body fields
--------------------

- *string* ``type``

    The type of event. This is a string containing the *category* and
    *action* separated by a dash delimiter (e.g., ``task-succeeded``).

- *string* ``hostname``

    The fully qualified hostname of where the event occurred at.

- *unsigned long long* ``clock``

    The logical clock value for this event (Lamport time-stamp).

- *float* ``timestamp``

    The UNIX time-stamp corresponding to the time of when the event occurred.

- *signed short* ``utcoffset``

    This field describes the timezone of the originating host, and is
    specified as the number of hours ahead of/behind UTC (e.g., -2 or
    +1).

- *unsigned long long* ``pid``

    The process id of the process the event originated in.

Standard event types
--------------------

For a list of standard event types and their fields see the
:ref:`event-reference`.

Example message
---------------

This is the message fields for a ``task-succeeded`` event:

.. code-block:: python

    properties = {
        'routing_key': 'task.succeeded',
        'exchange': 'celeryev',
        'content_type': 'application/json',
        'content_encoding': 'utf-8',
        'delivery_mode': 1,
    }
    headers = {
        'hostname': 'worker1@george.vandelay.com',
    }
    body = {
        'type': 'task-succeeded',
        'hostname': 'worker1@george.vandelay.com',
        'pid': 6335,
        'clock': 393912923921,
        'timestamp': 1401717709.101747,
        'utcoffset': -1,
        'uuid': '9011d855-fdd1-4f8f-adb3-a413b499eafb',
        'retval': '4',
        'runtime': 0.0003212,
    )
