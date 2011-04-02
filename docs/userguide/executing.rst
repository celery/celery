.. _guide-executing:

=================
 Executing Tasks
=================

.. contents::
    :local:


.. _executing-basics:

Basics
======

Executing a task is done with :meth:`~celery.task.Base.Task.apply_async`,
and the shortcut: :meth:`~celery.task.Base.Task.delay`.

`delay` is simple and convenient, as it looks like calling a regular
function:

.. code-block:: python

    Task.delay(arg1, arg2, kwarg1="x", kwarg2="y")

The same using `apply_async` is written like this:

.. code-block:: python

    Task.apply_async(args=[arg1, arg2], kwargs={"kwarg1": "x", "kwarg2": "y"})


While `delay` is convenient, it doesn't give you as much control as using
`apply_async`.  With `apply_async` you can override the execution options
available as attributes on the `Task` class (see :ref:`task-options`).
In addition you can set countdown/eta, task expiry, provide a custom broker
connection and more.

Let's go over these in more detail.  All the examples uses a simple task
called `add`, returning the sum of two positional arguments:

.. code-block:: python

    @task
    def add(x, y):
        return x + y

.. note::

    You can also execute a task by name using
    :func:`~celery.execute.send_task`, if you don't have access to the
    task class::

        >>> from celery.execute import send_task
        >>> result = send_task("tasks.add", [2, 2])
        >>> result.get()
        4

.. _executing-eta:

ETA and countdown
=================

The ETA (estimated time of arrival) lets you set a specific date and time that
is the earliest time at which your task will be executed.  `countdown` is
a shortcut to set eta by seconds into the future.

.. code-block:: python

    >>> result = add.apply_async(args=[10, 10], countdown=3)
    >>> result.get()    # this takes at least 3 seconds to return
    20

The task is guaranteed to be executed at some time *after* the
specified date and time, but not necessarily at that exact time.
Possible reasons for broken deadlines may include many items waiting
in the queue, or heavy network latency.  To make sure your tasks
are executed in a timely manner you should monitor queue lengths. Use
Munin, or similar tools, to receive alerts, so appropriate action can be
taken to ease the workload.  See :ref:`monitoring-munin`.

While `countdown` is an integer, `eta` must be a :class:`~datetime.datetime`
object, specifying an exact date and time (including millisecond precision,
and timezone information):

.. code-block:: python

    >>> from datetime import datetime, timedelta

    >>> tomorrow = datetime.now() + timedelta(days=1)
    >>> add.apply_async(args=[10, 10], eta=tomorrow)

.. _executing-expiration:

Expiration
==========

The `expires` argument defines an optional expiry time,
either as seconds after task publish, or a specific date and time using
:class:`~datetime.datetime`:

.. code-block:: python

    >>> # Task expires after one minute from now.
    >>> add.apply_async(args=[10, 10], expires=60)

    >>> # Also supports datetime
    >>> from datetime import datetime, timedelta
    >>> add.apply_async(args=[10, 10], kwargs,
    ...                 expires=datetime.now() + timedelta(days=1)


When a worker receives an expired task it will mark
the task as :state:`REVOKED` (:exc:`~celery.exceptions.TaskRevokedError`).

.. _executing-serializers:

Serializers
===========

Data transferred between clients and workers needs to be serialized.
The default serializer is :mod:`pickle`, but you can
change this globally or for each individual task.
There is built-in support for :mod:`pickle`, `JSON`, `YAML`
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

The client uses the following order to decide which serializer
to use when sending a task:

    1. The `serializer` argument to `apply_async`
    2. The tasks `serializer` attribute
    3. The default :setting:`CELERY_TASK_SERIALIZER` setting.


* Using the `serializer` argument to `apply_async`:

.. code-block:: python

    >>> add.apply_async(args=[10, 10], serializer="json")

.. _executing-connections:

Connections and connection timeouts.
====================================

Currently there is no support for broker connection pools, so 
`apply_async` establishes and closes a new connection every time
it is called.  This is something you need to be aware of when sending
more than one task at a time.

You handle the connection manually by creating a
publisher:

.. code-block:: python

    numbers = [(2, 2), (4, 4), (8, 8), (16, 16)]

    results = []
    publisher = add.get_publisher()
    try:
        for args in numbers:
            res = add.apply_async(args=args, publisher=publisher)
            results.append(res)
    finally:
        publisher.close()
        publisher.connection.close()

    print([res.get() for res in results])


.. note::

    This particular example is better expressed as a task set.
    See :ref:`sets-taskset`.  Tasksets already reuses connections.


The connection timeout is the number of seconds to wait before giving up
on establishing the connection.  You can set this by using the
`connect_timeout` argument to `apply_async`:

.. code-block:: python

    add.apply_async([10, 10], connect_timeout=3)

Or if you handle the connection manually:

.. code-block:: python

    publisher = add.get_publisher(connect_timeout=3)

.. _executing-routing:

Routing options
===============

Celery uses the AMQP routing mechanisms to route tasks to different workers.

Messages (tasks) are sent to exchanges, a queue binds to an exchange with a
routing key. Let's look at an example:

Let's pretend we have an application with lot of different tasks: some
process video, others process images, and some gather collective intelligence
about its users.  Some of these tasks are more important, so we want to make
sure the high priority tasks get sent to dedicated nodes.

For the sake of this example we have a single exchange called `tasks`.
There are different types of exchanges, each type interpreting the routing
key in different ways, implementing different messaging scenarios.

The most common types used with Celery are `direct` and `topic`.

* direct

    Matches the routing key exactly.

* topic

    In the topic exchange the routing key is made up of words separated by
    dots (`.`).  Words can be matched by the wild cards `*` and `#`,
    where `*` matches one exact word, and `#` matches one or many words.

    For example, `*.stock.#` matches the routing keys `usd.stock` and
    `euro.stock.db` but not `stock.nasdaq`.

We create three queues, `video`, `image` and `lowpri` that binds to
the `tasks` exchange.  For the queues we use the following binding keys::

    video: video.#
    image: image.#
    lowpri: misc.#

Now we can send our tasks to different worker machines, by making the workers
listen to different queues:

.. code-block:: python

    >>> add.apply_async(args=[filename],
    ...                               routing_key="video.compress")

    >>> add.apply_async(args=[filename, 360],
    ...                             routing_key="image.rotate")

    >>> add.apply_async(args=[filename, selection],
    ...                           routing_key="image.crop")
    >>> add.apply_async(routing_key="misc.recommend")


Later, if the crop task is consuming a lot of resources,
we can bind new workers to handle just the `"image.crop"` task,
by creating a new queue that binds to `"image.crop`".

.. seealso::

    To find out more about routing, please see :ref:`guide-routing`.

.. _executing-amq-opts:

AMQP options
============

* mandatory

This sets the delivery to be mandatory.  An exception will be raised
if there are no running workers able to take on the task.

Not supported by :mod:`amqplib`.

* immediate

Request immediate delivery. Will raise an exception
if the task cannot be routed to a worker immediately.

Not supported by :mod:`amqplib`.

* priority

A number between `0` and `9`, where `0` is the highest priority.

.. note::

    RabbitMQ does not yet support AMQP priorities.
