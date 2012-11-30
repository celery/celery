.. _guide-calling:

===============
 Calling Tasks
===============

.. contents::
    :local:
    :depth: 1


.. _calling-basics:

Basics
======

This document describes Celery's uniform "Calling API"
used by task instances and the :ref:`canvas <guide-canvas>`.

The API defines a standard set of execution options, as well as three methods:

    - ``apply_async(args[, kwargs[, ...]])``

        Sends a task message.

    - ``delay(*args, **kwargs)``

        Shortcut to send a task message, but does not support execution
        options.

    - *calling* (``__call__``)

        Applying an object supporting the calling API (e.g. ``add(2, 2)``)
        means that the task will be executed in the current process, and
        not by a worker (a message will not be sent).

.. _calling-cheat:

.. topic:: Quick Cheat Sheet

    - ``T.delay(arg, kwarg=value)``
        always a shortcut to ``.apply_async``.

    - ``T.apply_async((arg, ), {'kwarg': value})``

    - ``T.apply_async(countdown=10)``
        executes 10 seconds from now.

    - ``T.apply_async(eta=now + timedelta(seconds=10))``
        executes 10 seconds from now, specifed using ``eta``

    - ``T.apply_async(countdown=60, expires=120)``
        executes in one minute from now, but expires after 2 minutes.

    - ``T.apply_async(expires=now + timedelta(days=2))``
        expires in 2 days, set using :class:`~datetime.datetime`.


Example
-------

The :meth:`~@Task.delay` method is convenient as it looks like calling a regular
function:

.. code-block:: python

    task.delay(arg1, arg2, kwarg1='x', kwarg2='y')

Using :meth:`~@Task.apply_async` instead you have to write:

.. code-block:: python

    task.apply_async(args=[arg1, arg2], kwargs={'kwarg1': 'x', 'kwarg2': 'y'})

.. sidebar:: Tip

    If the task is not registered in the current process
    you can use :meth:`~@send_task` to call the task by name instead.


So `delay` is clearly convenient, but if you want to set additional execution
options you have to use ``apply_async``.

The rest of this document will go into the task execution
options in detail.  All examples use a task
called `add`, returning the sum of two arguments:

.. code-block:: python

    @celery.task
    def add(x, y):
        return x + y


.. topic:: There's another way...

    You will learn more about this later while reading about the :ref:`Canvas
    <guide-canvas>`, but :class:`~celery.subtask`'s are objects used to pass around
    the signature of a task invocation, (for example to send it over the
    network), and they also support the Calling API:

    .. code-block:: python

        task.s(arg1, arg2, kwarg1='x', kwargs2='y').apply_async()

.. _calling-links:

Linking (callbacks/errbacks)
============================

Celery supports linking tasks together so that one task follows another.
The callback task will be applied with the result of the parent task
as a partial argument:

.. code-block:: python

    add.apply_async((2, 2), link=add.s(16))

.. sidebar:: What is ``s``?

    The ``add.s`` call used here is called a subtask, I talk
    more about subtasks in the :ref:`canvas guide <guide-canvas>`,
    where you can also learn about :class:`~celery.chain`, which
    is a simpler way to chain tasks together.

    In practice the ``link`` execution option is considered an internal
    primitive, and you will probably not use it directly, but
    rather use chains instead.

Here the result of the first task (4) will be sent to a new
task that adds 16 to the previous result, forming the expression
:math:`(2 + 2) + 16 = 20`


You can also cause a callback to be applied if task raises an exception
(*errback*), but this behaves differently from a regular callback
in that it will be passed the id of the parent task, not the result.
This is because it may not always be possible to serialize
the exception raised, and so this way the error callback requires
a result backend to be enabled, and the task must retrieve the result
of the task instead.

This is an example error callback:

.. code-block:: python

    @celery.task
    def error_handler(uuid):
        result = AsyncResult(uuid)
        exc = result.get(propagate=False)
        print('Task %r raised exception: %r\n%r' % (
              exc, result.traceback))

it can be added to the task using the ``link_error`` execution
option:

.. code-block:: python

    add.apply_async((2, 2), link_error=error_handler.s())


In addition, both the ``link`` and ``link_error`` options can be expressed
as a list::

    add.apply_async((2, 2), link=[add.s(16), other_task.s()])

The callbacks/errbacks will then be called in order, and all
callbacks will be called with the return value of the parent task
as a partial argument.

.. _calling-eta:

ETA and countdown
=================

The ETA (estimated time of arrival) lets you set a specific date and time that
is the earliest time at which your task will be executed.  `countdown` is
a shortcut to set eta by seconds into the future.

.. code-block:: python

    >>> result = add.apply_async((2, 2), countdown=3)
    >>> result.get()    # this takes at least 3 seconds to return
    20

The task is guaranteed to be executed at some time *after* the
specified date and time, but not necessarily at that exact time.
Possible reasons for broken deadlines may include many items waiting
in the queue, or heavy network latency.  To make sure your tasks
are executed in a timely manner you should monitor the queue for congestion. Use
Munin, or similar tools, to receive alerts, so appropriate action can be
taken to ease the workload.  See :ref:`monitoring-munin`.

While `countdown` is an integer, `eta` must be a :class:`~datetime.datetime`
object, specifying an exact date and time (including millisecond precision,
and timezone information):

.. code-block:: python

    >>> from datetime import datetime, timedelta

    >>> tomorrow = datetime.utcnow() + timedelta(days=1)
    >>> add.apply_async((2, 2), eta=tomorrow)

.. _calling-expiration:

Expiration
==========

The `expires` argument defines an optional expiry time,
either as seconds after task publish, or a specific date and time using
:class:`~datetime.datetime`:

.. code-block:: python

    >>> # Task expires after one minute from now.
    >>> add.apply_async((10, 10), expires=60)

    >>> # Also supports datetime
    >>> from datetime import datetime, timedelta
    >>> add.apply_async((10, 10), kwargs,
    ...                 expires=datetime.now() + timedelta(days=1)


When a worker receives an expired task it will mark
the task as :state:`REVOKED` (:exc:`~@TaskRevokedError`).

.. _calling-retry:

Message Sending Retry
=====================

Celery will automatically retry sending messages in the event of connection
failure, and retry behavior can be configured -- like how often to retry, or a maximum
number of retries -- or disabled all together.

To disable retry you can set the ``retry`` execution option to :const:`False`:

.. code-block:: python

    add.apply_async((2, 2), retry=False)

.. topic:: Related Settings

    .. hlist::
        :columns: 2

        - :setting:`CELERY_TASK_PUBLISH_RETRY`
        - :setting:`CELERY_TASK_PUBLISH_RETRY_POLICY`

Retry Policy
------------

A retry policy is a mapping that controls how retries behave,
and can contain the following keys:

- `max_retries`

    Maximum number of retries before giving up, in this case the
    exception that caused the retry to fail will be raised.

    A value of 0 or :const:`None` means it will retry forever.

    The default is to retry 3 times.

- `interval_start`

    Defines the number of seconds (float or integer) to wait between
    retries.  Default is 0, which means the first retry will be
    instantaneous.

- `interval_step`

    On each consecutive retry this number will be added to the retry
    delay (float or integer).  Default is 0.2.

- `interval_max`

    Maximum number of seconds (float or integer) to wait between
    retries.  Default is 0.2.

For example, the default policy correlates to:

.. code-block:: python

    add.apply_async((2, 2), retry=True, retry_policy={
        'max_retries': 3,
        'interval_start': 0,
        'interval_step': 0.2,
        'interval_max': 0.2,
    })

the maximum time spent retrying will be 0.4 seconds.  It is set relatively
short by default because a connection failure could lead to a retry pile effect
if the broker connection is down: e.g. many web server processes waiting
to retry blocking other incoming requests.

.. _calling-serializers:

Serializers
===========

.. sidebar::  Security

    The pickle module allows for execution of arbitrary functions,
    please see the :ref:`security guide <guide-security>`.

    Celery also comes with a special serializer that uses
    cryptography to sign your messages.

Data transferred between clients and workers needs to be serialized,
so every message in Celery has a ``content_type`` header that
describes the serialization method used to encode it.

The default serializer is :mod:`pickle`, but you can
change this using the :setting:`CELERY_TASK_SERIALIZER` setting,
or for each individual task, or even per message.

There's built-in support for :mod:`pickle`, `JSON`, `YAML`
and `msgpack`, and you can also add your own custom serializers by registering
them into the Kombu serializer registry (see `Kombu: Serialization of Data`_).

.. _`Kombu: Serialization of Data`:
    http://packages.python.org/kombu/introduction.html#serialization-of-data

Each option has its advantages and disadvantages.

json -- JSON is supported in many programming languages, is now
    a standard part of Python (since 2.6), and is fairly fast to decode
    using the modern Python libraries such as :mod:`cjson` or :mod:`simplejson`.

    The primary disadvantage to JSON is that it limits you to the following
    data types: strings, Unicode, floats, boolean, dictionaries, and lists.
    Decimals and dates are notably missing.

    Also, binary data will be transferred using Base64 encoding, which will
    cause the transferred data to be around 34% larger than an encoding which
    supports native binary types.

    However, if your data fits inside the above constraints and you need
    cross-language support, the default setting of JSON is probably your
    best choice.

    See http://json.org for more information.

pickle -- If you have no desire to support any language other than
    Python, then using the pickle encoding will gain you the support of
    all built-in Python data types (except class instances), smaller
    messages when sending binary files, and a slight speedup over JSON
    processing.

    See http://docs.python.org/library/pickle.html for more information.

yaml -- YAML has many of the same characteristics as json,
    except that it natively supports more data types (including dates,
    recursive references, etc.)

    However, the Python libraries for YAML are a good bit slower than the
    libraries for JSON.

    If you need a more expressive set of data types and need to maintain
    cross-language compatibility, then YAML may be a better fit than the above.

    See http://yaml.org/ for more information.

msgpack -- msgpack is a binary serialization format that is closer to JSON
    in features.  It is very young however, and support should be considered
    experimental at this point.

    See http://msgpack.org/ for more information.

The encoding used is available as a message header, so the worker knows how to
deserialize any task.  If you use a custom serializer, this serializer must
be available for the worker.

The following order is used to decide which serializer
to use when sending a task:

    1. The `serializer` execution option.
    2. The :attr:`@-Task.serializer` attribute
    3. The :setting:`CELERY_TASK_SERIALIZER` setting.


Example setting a custom serializer for a single task invocation:

.. code-block:: python

    >>> add.apply_async((10, 10), serializer='json')

.. _calling-compression:

Compression
===========

Celery can compress the messages using either *gzip*, or *bzip2*.
You can also create your own compression schemes and register
them in the :func:`kombu compression registry <kombu.compression.register>`.

The following order is used to decide which compression scheme
to use when sending a task:

    1. The `compression` execution option.
    2. The :attr:`@-Task.compression` attribute.
    3. The :setting:`CELERY_MESSAGE_COMPRESSION` attribute.

Example specifying the compression used when calling a task::

    >>> add.apply_async((2, 2), compression='zlib')

.. _calling-connections:

Connections
===========

.. sidebar:: Automatic Pool Support

    Since version 2.3 there is support for automatic connection pools,
    so you don't have to manually handle connections and publishers
    to reuse connections.

    The connection pool is enabled by default since version 2.5.

    See the :setting:`BROKER_POOL_LIMIT` setting for more information.

You can handle the connection manually by creating a
publisher:

.. code-block:: python


    results = []
    with add.app.pool.acquire(block=True) as connection:
        with add.get_publisher(connection) as publisher:
            try:
                for args in numbers:
                    res = add.apply_async((2, 2), publisher=publisher)
                    results.append(res)
    print([res.get() for res in results])


Though this particular example is much better expressed as a group:

.. code-block:: python

    >>> from celery import group

    >>> numbers = [(2, 2), (4, 4), (8, 8), (16, 16)]
    >>> res = group(add.subtask(n) for i in numbers).apply_async()

    >>> res.get()
    [4, 8, 16, 32]

.. _calling-routing:

Routing options
===============

Celery can route tasks to different queues.

Simple routing (name <-> name) is accomplished using the ``queue`` option::

    add.apply_async(queue='priority.high')

You can then assign workers to the ``priority.high`` queue by using
the workers :option:`-Q` argument:

.. code-block:: bash

    $ celery worker -l info -Q celery,priority.high

.. seealso::

    Hard-coding queue names in code is not recommended, the best practice
    is to use configuration routers (:setting:`CELERY_ROUTES`).

    To find out more about routing, please see :ref:`guide-routing`.

Advanced Options
----------------

These options are for advanced users who want to take use of
AMQP's full routing capabilities. Interested parties may read the
:ref:`routing guide <guide-routing>`.

- exchange

    Name of exchange (or a :class:`kombu.entity.Exchange`) to
    send the message to.

- routing_key

    Routing key used to determine.

- priority

    A number between `0` and `9`, where `0` is the highest priority.

    Supported by: redis, beanstalk
