.. _guide-extending:

==========================
 Extensions and Bootsteps
==========================

.. contents::
    :local:
    :depth: 2

.. _extending-bootsteps:

Bootsteps
=========

Blahblah blah, example bootstep:

.. code-block:: python

    from celery import Celery
    from celery import bootsteps

    class InfoStep(bootsteps.Step):

        def __init__(self, parent, **kwargs):
            # here we can prepare the Worker/Consumer object
            # in any way we want, set attribute defaults and so on.
            print('{0!r} is in init'.format(parent))

        def start(self, parent):
            # our step is started together with all other Worker/Consumer
            # bootsteps.
            print('{0!r} is starting'.format(parent))

        def stop(self, parent):
            # the Consumer calls stop every time the consumer is restarted
            # (i.e. connection is lost) and also at shutdown.  The Worker
            # will call stop at shutdown only.
            print('{0!r} is stopping'.format(parent))

        def shutdown(self, parent):
            # shutdown is called by the Consumer at shutdown, it's not
            # called by Worker.
            print('{0!r} is shutting down'.format(parent))

        app = Celery(broker='amqp://')
        app.steps['worker'].add(InfoStep)
        app.steps['consumer'].add(InfoStep)

Starting the worker with this step installed will give us the following
logs::

    <celery.apps.worker.Worker object at 0x101ad8410> is in init
    <celery.worker.consumer.Consumer object at 0x101c2d790> is in init
    [2013-05-29 16:18:20,544: WARNING/MainProcess]
        <celery.apps.worker.Worker object at 0x101ad8410> is starting
    [2013-05-29 16:18:21,577: WARNING/MainProcess]
        <celery.worker.consumer.Consumer object at 0x101c2d8d0> is starting
    <celery.worker.consumer.Consumer object at 0x101c2d790> is stopping
    <celery.apps.worker.Worker object at 0x101ad8410> is stopping
    <celery.worker.consumer.Consumer object at 0x101c2d790> is shutting down

The ``print`` statements will be redirected to the logging subsystem after
the worker has been initialized, so the "is starting" lines are timestamped.
You may notice that this does no longer happen at shutdown, this is because
the ``stop`` and ``shutdown`` methods are called inside a *signal handler*,
and it's not safe to use logging inside such a handler.
Logging with the Python logging module is not :term:`reentrant`,
which means that you cannot interrupt the function and
call it again later.  It's important that the ``stop`` and ``shutdown`` methods
you write is also :term:`reentrant`.

Starting the worker with ``--loglevel=debug`` will show us more
information about the boot process::

    [2013-05-29 16:18:20,509: DEBUG/MainProcess] | Worker: Preparing bootsteps.
    [2013-05-29 16:18:20,511: DEBUG/MainProcess] | Worker: Building graph...
    <celery.apps.worker.Worker object at 0x101ad8410> is in init
    [2013-05-29 16:18:20,511: DEBUG/MainProcess] | Worker: New boot order:
        {Hub, Queues (intra), Pool, Autoreloader, Timer, StateDB,
         Autoscaler, InfoStep, Beat, Consumer}
    [2013-05-29 16:18:20,514: DEBUG/MainProcess] | Consumer: Preparing bootsteps.
    [2013-05-29 16:18:20,514: DEBUG/MainProcess] | Consumer: Building graph...
    <celery.worker.consumer.Consumer object at 0x101c2d8d0> is in init
    [2013-05-29 16:18:20,515: DEBUG/MainProcess] | Consumer: New boot order:
        {Connection, Mingle, Events, Gossip, InfoStep, Agent,
         Heart, Control, Tasks, event loop}
    [2013-05-29 16:18:20,522: DEBUG/MainProcess] | Worker: Starting Hub
    [2013-05-29 16:18:20,522: DEBUG/MainProcess] ^-- substep ok
    [2013-05-29 16:18:20,522: DEBUG/MainProcess] | Worker: Starting Pool
    [2013-05-29 16:18:20,542: DEBUG/MainProcess] ^-- substep ok
    [2013-05-29 16:18:20,543: DEBUG/MainProcess] | Worker: Starting InfoStep
    [2013-05-29 16:18:20,544: WARNING/MainProcess]
        <celery.apps.worker.Worker object at 0x101ad8410> is starting
    [2013-05-29 16:18:20,544: DEBUG/MainProcess] ^-- substep ok
    [2013-05-29 16:18:20,544: DEBUG/MainProcess] | Worker: Starting Consumer
    [2013-05-29 16:18:20,544: DEBUG/MainProcess] | Consumer: Starting Connection
    [2013-05-29 16:18:20,559: INFO/MainProcess] Connected to amqp://guest@127.0.0.1:5672//
    [2013-05-29 16:18:20,560: DEBUG/MainProcess] ^-- substep ok
    [2013-05-29 16:18:20,560: DEBUG/MainProcess] | Consumer: Starting Mingle
    [2013-05-29 16:18:20,560: INFO/MainProcess] mingle: searching for neighbors
    [2013-05-29 16:18:21,570: INFO/MainProcess] mingle: no one here
    [2013-05-29 16:18:21,570: DEBUG/MainProcess] ^-- substep ok
    [2013-05-29 16:18:21,571: DEBUG/MainProcess] | Consumer: Starting Events
    [2013-05-29 16:18:21,572: DEBUG/MainProcess] ^-- substep ok
    [2013-05-29 16:18:21,572: DEBUG/MainProcess] | Consumer: Starting Gossip
    [2013-05-29 16:18:21,577: DEBUG/MainProcess] ^-- substep ok
    [2013-05-29 16:18:21,577: DEBUG/MainProcess] | Consumer: Starting InfoStep
    [2013-05-29 16:18:21,577: WARNING/MainProcess]
        <celery.worker.consumer.Consumer object at 0x101c2d8d0> is starting
    [2013-05-29 16:18:21,578: DEBUG/MainProcess] ^-- substep ok
    [2013-05-29 16:18:21,578: DEBUG/MainProcess] | Consumer: Starting Heart
    [2013-05-29 16:18:21,579: DEBUG/MainProcess] ^-- substep ok
    [2013-05-29 16:18:21,579: DEBUG/MainProcess] | Consumer: Starting Control
    [2013-05-29 16:18:21,583: DEBUG/MainProcess] ^-- substep ok
    [2013-05-29 16:18:21,583: DEBUG/MainProcess] | Consumer: Starting Tasks
    [2013-05-29 16:18:21,606: DEBUG/MainProcess] basic.qos: prefetch_count->80
    [2013-05-29 16:18:21,606: DEBUG/MainProcess] ^-- substep ok
    [2013-05-29 16:18:21,606: DEBUG/MainProcess] | Consumer: Starting event loop
    [2013-05-29 16:18:21,608: WARNING/MainProcess] celery@example.com ready.

.. figure:: ../images/worker_graph_full.png

.. _extending-worker-bootsteps:

Worker bootsteps
----------------

Blablah

.. _extending-consumer-bootsteps:

Consumer bootsteps
------------------

blahblah


.. _extending-programs:

Command-line programs
=====================

.. _extending-commandoptions:

Adding new command-line options
-------------------------------

You can add additional command-line options to the ``worker``, ``beat`` and
``events`` commands by modifying the :attr:`~@Celery.user_options` attribute of the
application instance.

Celery commands uses the :mod:`optparse` module to parse command-line
arguments, and so you have to use optparse specific option instances created
using :func:`optparse.make_option`.  Please see the :mod:`optparse`
documentation to read about the fields supported.

Example adding a custom option to the :program:`celery worker` command:

.. code-block:: python

    from celery import Celery
    from optparse import make_option as Option

    celery = Celery(broker='amqp://')

    celery.user_options['worker'].add(
        Option('--enable-my-option', action='store_true', default=False,
               help='Enable custom option.'),
    )

.. _extending-subcommands:

Adding new :program:`celery` sub-commands
-----------------------------------------

New commands can be added to the :program:`celery` umbrella command by using
`setuptools entry-points`_.

.. _`setuptools entry-points`:
    http://reinout.vanrees.org/weblog/2010/01/06/zest-releaser-entry-points.html


Entry-points is special metadata that can be added to your packages ``setup.py`` program,
and then after installation, read from the system using the :mod:`pkg_resources` module.

Celery recognizes ``celery.commands`` entry-points to install additional
subcommands, where the value of the entry-point must point to a valid subclass
of :class:`celery.bin.base.Command`.  Sadly there is limited documentation,
but you can find inspiration from the various commands in the
:mod:`celery.bin` package.

This is how the Flower_ monitoring extension adds the :program:`celery flower` command,
by adding an entry-point in :file:`setup.py`:

.. code-block:: python

    setup(
        name='flower',
        entry_points={
            'celery.commands': [
               'flower = flower.command.FlowerCommand',
            ],
        }
    )


.. _Flower: http://pypi.python.org/pypi/flower

The command definition is in two parts separated by the equal sign, where the
first part is the name of the subcommand (flower), then the fully qualified
module path to the class that implements the command
(``flower.command.FlowerCommand``).


In the module :file:`flower/command.py`, the command class is defined
something like this:

.. code-block:: python

    from celery.bin.base import Command, Option


    class FlowerCommand(Command):

        def get_options(self):
            return (
                Option('--port', default=8888, type='int',
                    help='Webserver port',
                ),
                Option('--debug', action='store_true'),
            )

        def run(self, port=None, debug=False, **kwargs):
            print('Running our command')


Worker API
==========


:class:`~celery.worker.Hub` - The workers async event loop.
-----------------------------------------------------------
:supported transports: amqp, redis

.. versionadded:: 3.0

The worker uses asynchronous I/O when the amqp or redis broker transports are
used.  The eventual goal is for all transports to use the eventloop, but that
will take some time so other transports still use a threading-based solution.

.. method:: hub.add(fd, callback, flags)

    Add callback for fd with custom flags, which can be any combination of
    :data:`~kombu.utils.eventio.READ`, :data:`~kombu.utils.eventio.WRITE`,
    and :data:`~kombu.utils.eventio.ERR`, the callback will then be called
    whenever the condition specified in flags is true (readable,
    writeable, or error).

    The callback will stay registered until explictly removed using
    :meth:`hub.remove(fd) <hub.remove>`, or the fd is automatically discarded
    because it's no longer valid.

    Note that only one callback can be registered for any given fd at a time,
    so calling ``add`` a second time will remove any callback that
    was previously registered for that fd.

    ``fd`` may also be a list of file descriptors, in this case the
    callback will be registered for all of the fds in this list.

    A file descriptor is any file-like object that supports the ``fileno``
    method, or it can be the file descriptor number (int).

.. method:: hub.add_reader(fd, callback)

    Shortcut to ``hub.add(fd, callback, READ | ERR)``.

.. method:: hub.add_writer(fd, callback)

    Shortcut to ``hub.add(fd, callback, WRITE)``.

.. method:: hub.remove(fd)

    Remove all callbacks for ``fd`` from the loop.

.. method:: hub.update_readers(fd, mapping)

    Shortcut to add callbacks from a map of ``{fd: callback}`` items.

.. method:: hub.update_writers(fd, mapping)

    Shortcut to add callbacks from a map of ``{fd: callback}`` items.

Timer - Scheduling events
-------------------------

.. method:: timer.apply_after(msecs, callback, args=(), kwargs=(),
                              priority=0)


.. method:: timer.apply_interval(msecs, callback, args=(), kwargs=(),
                                priority=0)

.. method:: timer.apply_at(eta, callback, args=(), kwargs=(),
                           priority=0)
