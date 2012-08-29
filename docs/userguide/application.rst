.. _guide-app:

=============
 Application
=============

.. contents::
    :local:
    :depth: 1

The Celery library must be instantiated before use, this instance
is called an application (or *app* for short).

The application is thread-safe so that multiple Celery applications
with different configuration, components and tasks can co-exist in the
same process space.

Let's create one now:

.. code-block:: python

    >>> from celery import Celery
    >>> celery = Celery()
    >>> celery
    <Celery __main__:0x100469fd0>

The last line shows the textual representation of the application,
which includes the name of the celery class (``Celery``), the name of the
current main module (``__main__``), and the memory address of the object
(``0x100469fd0``).

Main Name
=========

Only one of these is important, and that is the main module name,
let's look at why that is.

When you send a task message in Celery, that message will not contain
any source code, but only the name of the task you want to execute.
This works similarly to how host names works on the internet: every worker
maintains a mapping of task names to their actual functions, called the *task
registry*.

Whenever you define a task, that task will also be added to the local registry:

.. code-block:: python

    >>> @celery.task
    ... def add(x, y):
    ...     return x + y

    >>> add
    <@task: __main__.add>

    >>> add.name
    __main__.add

    >>> celery.tasks['__main__.add']
    <@task: __main__.add>

and there you see that ``__main__`` again; whenever Celery is not able
to detect what module the function belongs to, it uses the main module
name to generate the beginning of the task name.

This is only a problem in a limited set of use cases:

    #. If the module that the task is defined in is run as a program.
    #. If the application is created in the Python shell (REPL).

For example here, where the tasks module is also used to start a worker:

:file:`tasks.py`:

.. code-block:: python

    from celery import Celery
    celery = Celery()

    @celery.task
    def add(x, y): return x + y

    if __name__ == '__main__':
        celery.worker_main()

When this module is executed the tasks will be named starting with "``__main__``",
but when it the module is imported by another process, say to call a task,
the tasks will be named starting with "``tasks``" (the real name of the module)::

    >>> from tasks import add
    >>> add.name
    tasks.add

You can specify another name for the main module:

.. code-block:: python

    >>> celery = Celery('tasks')
    >>> celery.main
    'tasks'

    >>> @celery.task
    ... def add(x, y):
    ...     return x + y

    >>> add.name
    tasks.add

.. seealso:: :ref:`task-names`

Configuration
=============

There are lots of different options you can set that will change how
Celery work.  These options can be set on the app instance directly,
or you can use a dedicated configuration module.

The configuration is available as :attr:`@Celery.conf`::

    >>> celery.conf.CELERY_TIMEZONE
    'Europe/London'

where you can set configuration values directly::

    >>> celery.conf.CELERY_ENABLE_UTC = True

or you can update several keys at once by using the ``update`` method::

    >>> celery.conf.update(
    ...     CELERY_ENABLE_UTC=True,
    ...     CELERY_TIMEZONE='Europe/London',
    ...)

The configuration object consists of multiple dictionaries
that are consulted in order:

    #. Changes made at runtime.
    #. The configuration module (if any)
    #. The default configuration (:mod:`celery.app.defaults`).


.. seealso::

    Go to the :ref:`Configuration reference <configuration>` for a complete
    listing of all the available settings, and their default values.

``config_from_object``
----------------------

.. sidebar:: Timezones & pytz

    Setting a time zone other than UTC requires the :mod:`pytz` library
    to be installed, see the :setting:`CELERY_TIMEZONE` setting for more
    information.


The :meth:`@Celery.config_from_object` method loads configuration
from a configuration object.

This can be a configuration module, or any object with configuration attributes.

Note that any configuration that was previous set will be reset when
:meth:`~@Celery.config_from_object` is called.  If you want to set additional
configuration you should do so after.

Example 1: Using the name of a module
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from celery import Celery

    celery = Celery()
    celery.config_from_object('celeryconfig')


The ``celeryconfig`` module may then look like this:

:file:`celeryconfig.py`:

.. code-block:: python

    CELERY_ENABLE_UTC = True
    CELERY_TIMEZONE = 'Europe/London'

Example 2: Using a configuration module
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from celery import Celery

    celery = Celery()
    import celeryconfig
    celery.config_from_object(celeryconfig)

Example 3:  Using a configuration class/object
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from celery import Celery

    celery = Celery()

    class Config:
        CELERY_ENABLE_UTC = True
        CELERY_TIMEZONE = 'Europe/London'

    celery.config_from_object(Config)

``config_from_envvar``
----------------------

The :meth:`@Celery.config_from_envvar` takes the configuration module name
from an environment variable

For example -- to load configuration from a module specified in the
environment variable named :envvar:`CELERY_CONFIG_MODULE`:

.. code-block:: python

    import os
    from celery import Celery

    #: Set default configuration module name
    os.environ.setdefault('CELERY_CONFIG_MODULE', 'celeryconfig')

    celery = Celery()
    celery.config_from_envvar('CELERY_CONFIG_MODULE')

You can then specify the configuration module to use via the environment:

.. code-block:: bash

    $ CELERY_CONFIG_MODULE="celeryconfig.prod" celery worker -l info

Laziness
========

The application instance is lazy, meaning that it will not be evaluated
until something is actually needed.

Creating a :class:`@Celery` instance will only do the following:

    #. Create a logical clock instance, used for events.
    #. Create the task registry.
    #. Set itself as the current app (but not if the ``set_as_current``
       argument was disabled)
    #. Call the :meth:`@Celery.on_init` callback (does nothing by default).

The :meth:`~@Celery.task` decorator does not actually create the
tasks at the point when it's called, instead it will defer the creation
of the task to happen either when the task is used, or after the
application has been *finalized*,

This example shows how the task is not created until
you use the task, or access an attribute (in this case :meth:`repr`):

.. code-block:: python

    >>> @celery.task
    >>> def add(x, y):
    ...    return x + y

    >>> type(add)
    <class 'celery.local.PromiseProxy'>

    >>> add.__evaluated__()
    False

    >>> add        # <-- causes repr(add) to happen
    <@task: __main__.add>

    >>> add.__evaluated__()
    True

*Finalization* of the app happens either explicitly by calling
:meth:`@Celery.finalize` -- or implicitly by accessing the :attr:`~@Celery.tasks`
attribute.

Finalizing the object will:

    #. Copy tasks that must be shared between apps

        Tasks are shared by default, but if the
        ``shared`` argument to the task decorator is disabled,
        then the task will be private to the app it's bound to.

    #. Evaluate all pending task decorators.

    #. Make sure all tasks are bound to the current app.

        Tasks are bound to apps so that it can read default
        values from the configuration.

.. _default-app:

.. topic:: The "default app".

    Celery did not always work this way, it used to be that
    there was only a module-based API, and for backwards compatibility
    the old API is still there.

    Celery always creates a special app that is the "default app",
    and this is used if no custom application has been instantiated.

    The :mod:`celery.task` module is there to accommodate the old API,
    and should not be used if you use a custom app. You should
    always use the methods on the app instance, not the module based API.

    For example, the old Task base class enables many compatibility
    features where some may be incompatible with newer features, such
    as task methods:

    .. code-block:: python

        from celery.task import Task   # << OLD Task base class.

        from celery import Task        # << NEW base class.

    The new base class is recommended even if you use the old
    module-based API.


Breaking the chain
==================

While it's possible to depend on the current app
being set, the best practice is to always pass the app instance
around to anything that needs it.

I call this the "app chain", since it creates a chain
of instances depending on the app being passed.

The following example is considered bad practice:

.. code-block:: python

    from celery import current_app

    class Scheduler(object):

        def run(self):
            app = current_app

Instead it should take the ``app`` as an argument:

.. code-block:: python

    class Scheduler(object):

        def __init__(self, app):
            self.app = app

Internally Celery uses the :func:`celery.app.app_or_default` function
so that everything also works in the module-based compatibility API

.. code-block:: python

    from celery.app import app_or_default

    class Scheduler(object):
        def __init__(self, app=None):
            self.app = app_or_default(app)

In development you can set the :envvar:`CELERY_TRACE_APP`
environment variable to raise an exception if the app
chain breaks:

.. code-block:: bash

    $ CELERY_TRACE_APP=1 celery worker -l info


.. topic:: Evolving the API

    Celery has changed a lot in the 3 years since it was initially
    created.

    For example, in the beginning it was possible to use any callable as
    a task:

    .. code-block:: python

        def hello(to):
            return 'hello %s' % to

        >>> from celery.execute import apply_async

        >>> apply_async(hello, ('world!', ))

    or you could also create a ``Task`` class to set
    certain options, or override other behavior

    .. code-block:: python

        from celery.task import Task
        from celery.registry import tasks

        class Hello(Task):
            send_error_emails = True

            def run(self, to):
                return 'hello %s' % to
        tasks.register(Hello)

        >>> Hello.delay('world!')

    Later, it was decided that passing arbitrary call-ables
    was an anti-pattern, since it makes it very hard to use
    serializers other than pickle, and the feature was removed
    in 2.0, replaced by task decorators:

    .. code-block:: python

        from celery.task import task

        @task(send_error_emails=True)
        def hello(x):
            return 'hello %s' % to

Abstract Tasks
==============

All tasks created using the :meth:`~@Celery.task` decorator
will inherit from the applications base :attr:`~@Celery.Task` class.

You can specify a different base class with the ``base`` argument:

.. code-block:: python

    @celery.task(base=OtherTask):
    def add(x, y):
        return x + y

To create a custom task class you should inherit from the neutral base
class: :class:`celery.Task`.

.. code-block:: python

    from celery import Task

    class DebugTask(Task):
        abstract = True

        def __call__(self, *args, **kwargs):
            print('TASK STARTING: %s[%s]' % (self.name, self.request.id))
            return self.run(*args, **kwargs)


The neutral base class is special because it's not bound to any specific app
yet.  Concrete subclasses of this class will be bound, so you should
always mark generic base classes as ``abstract``

Once a task is bound to an app it will read configuration to set default values
and so on.

It's also possible to change the default base class for an application
by changing its :meth:`@Celery.Task` attribute:

.. code-block:: python

    >>> from celery import Celery, Task

    >>> celery = Celery()

    >>> class MyBaseTask(Task):
    ...    abstract = True
    ...    send_error_emails = True

    >>> celery.Task = MyBaseTask
    >>> celery.Task
    <unbound MyBaseTask>

    >>> @x.task
    ... def add(x, y):
    ...     return x + y

    >>> add
    <@task: __main__.add>

    >>> add.__class__.mro()
    [<class add of <Celery __main__:0x1012b4410>>,
     <unbound MyBaseTask>,
     <unbound Task>,
     <type 'object'>]
