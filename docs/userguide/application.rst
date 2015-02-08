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
    >>> app = Celery()
    >>> app
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

    >>> @app.task
    ... def add(x, y):
    ...     return x + y

    >>> add
    <@task: __main__.add>

    >>> add.name
    __main__.add

    >>> app.tasks['__main__.add']
    <@task: __main__.add>

and there you see that ``__main__`` again; whenever Celery is not able
to detect what module the function belongs to, it uses the main module
name to generate the beginning of the task name.

This is only a problem in a limited set of use cases:

    #. If the module that the task is defined in is run as a program.
    #. If the application is created in the Python shell (REPL).

For example here, where the tasks module is also used to start a worker
with :meth:`@worker_main`:

:file:`tasks.py`:

.. code-block:: python

    from celery import Celery
    app = Celery()

    @app.task
    def add(x, y): return x + y

    if __name__ == '__main__':
        app.worker_main()

When this module is executed the tasks will be named starting with "``__main__``",
but when the module is imported by another process, say to call a task,
the tasks will be named starting with "``tasks``" (the real name of the module)::

    >>> from tasks import add
    >>> add.name
    tasks.add

You can specify another name for the main module:

.. code-block:: python

    >>> app = Celery('tasks')
    >>> app.main
    'tasks'

    >>> @app.task
    ... def add(x, y):
    ...     return x + y

    >>> add.name
    tasks.add

.. seealso:: :ref:`task-names`

Configuration
=============

There are several options you can set that will change how
Celery works.  These options can be set directly on the app instance,
or you can use a dedicated configuration module.

The configuration is available as :attr:`@conf`::

    >>> app.conf.CELERY_TIMEZONE
    'Europe/London'

where you can also set configuration values directly::

    >>> app.conf.CELERY_ENABLE_UTC = True

and update several keys at once by using the ``update`` method::

    >>> app.conf.update(
    ...     CELERY_ENABLE_UTC=True,
    ...     CELERY_TIMEZONE='Europe/London',
    ...)

The configuration object consists of multiple dictionaries
that are consulted in order:

    #. Changes made at runtime.
    #. The configuration module (if any)
    #. The default configuration (:mod:`celery.app.defaults`).

You can even add new default sources by using the :meth:`@add_defaults`
method.

.. seealso::

    Go to the :ref:`Configuration reference <configuration>` for a complete
    listing of all the available settings, and their default values.

``config_from_object``
----------------------

The :meth:`@config_from_object` method loads configuration
from a configuration object.

This can be a configuration module, or any object with configuration attributes.

Note that any configuration that was previous set will be reset when
:meth:`~@config_from_object` is called.  If you want to set additional
configuration you should do so after.

Example 1: Using the name of a module
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from celery import Celery

    app = Celery()
    app.config_from_object('celeryconfig')


The ``celeryconfig`` module may then look like this:

:file:`celeryconfig.py`:

.. code-block:: python

    CELERY_ENABLE_UTC = True
    CELERY_TIMEZONE = 'Europe/London'

Example 2: Using a configuration module
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tip::

    Using the name of a module is recommended
    as this means that the module doesn't need to be serialized
    when the prefork pool is used.  If you're
    experiencing configuration pickle errors then please try using
    the name of a module instead.

.. code-block:: python

    from celery import Celery

    app = Celery()
    import celeryconfig
    app.config_from_object(celeryconfig)

Example 3:  Using a configuration class/object
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from celery import Celery

    app = Celery()

    class Config:
        CELERY_ENABLE_UTC = True
        CELERY_TIMEZONE = 'Europe/London'

    app.config_from_object(Config)
    # or using the fully qualified name of the object:
    #   app.config_from_object('module:Config')

``config_from_envvar``
----------------------

The :meth:`@config_from_envvar` takes the configuration module name
from an environment variable

For example -- to load configuration from a module specified in the
environment variable named :envvar:`CELERY_CONFIG_MODULE`:

.. code-block:: python

    import os
    from celery import Celery

    #: Set default configuration module name
    os.environ.setdefault('CELERY_CONFIG_MODULE', 'celeryconfig')

    app = Celery()
    app.config_from_envvar('CELERY_CONFIG_MODULE')

You can then specify the configuration module to use via the environment:

.. code-block:: bash

    $ CELERY_CONFIG_MODULE="celeryconfig.prod" celery worker -l info

.. _app-censored-config:

Censored configuration
----------------------

If you ever want to print out the configuration, as debugging information
or similar, you may also want to filter out sensitive information like
passwords and API keys.

Celery comes with several utilities used for presenting the configuration,
one is :meth:`~celery.app.utils.Settings.humanize`:

.. code-block:: python

    >>> app.conf.humanize(with_defaults=False, censored=True)

This method returns the configuration as a tabulated string.  This will
only contain changes to the configuration by default, but you can include the
default keys and values by changing the ``with_defaults`` argument.

If you instead want to work with the configuration as a dictionary, then you
can use the :meth:`~celery.app.utils.Settings.table` method:

.. code-block:: python

    >>> app.conf.table(with_defaults=False, censored=True)

Please note that Celery will not be able to remove all sensitive information,
as it merely uses a regular expression to search for commonly named keys.
If you add custom settings containing sensitive information you should name
the keys using a name that Celery identifies as secret.

A configuration setting will be censored if the name contains any of
these substrings:

``API``, ``TOKEN``, ``KEY``, ``SECRET``, ``PASS``, ``SIGNATURE``, ``DATABASE``

Laziness
========

The application instance is lazy, meaning that it will not be evaluated
until something is actually needed.

Creating a :class:`@Celery` instance will only do the following:

    #. Create a logical clock instance, used for events.
    #. Create the task registry.
    #. Set itself as the current app (but not if the ``set_as_current``
       argument was disabled)
    #. Call the :meth:`@on_init` callback (does nothing by default).

The :meth:`@task` decorator does not actually create the
tasks at the point when it's called, instead it will defer the creation
of the task to happen either when the task is used, or after the
application has been *finalized*,

This example shows how the task is not created until
you use the task, or access an attribute (in this case :meth:`repr`):

.. code-block:: python

    >>> @app.task
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
:meth:`@finalize` -- or implicitly by accessing the :attr:`@tasks`
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
            return 'hello {0}'.format(to)

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
                return 'hello {0}'.format(to)
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
            return 'hello {0}'.format(to)

Abstract Tasks
==============

All tasks created using the :meth:`~@task` decorator
will inherit from the applications base :attr:`~@Task` class.

You can specify a different base class with the ``base`` argument:

.. code-block:: python

    @app.task(base=OtherTask):
    def add(x, y):
        return x + y

To create a custom task class you should inherit from the neutral base
class: :class:`celery.Task`.

.. code-block:: python

    from celery import Task

    class DebugTask(Task):
        abstract = True

        def __call__(self, *args, **kwargs):
            print('TASK STARTING: {0.name}[{0.request.id}]'.format(self))
            return super(DebugTask, self).__call__(*args, **kwargs)


.. tip::

    If you override the tasks ``__call__`` method, then it's very important
    that you also call super so that the base call method can set up the
    default request used when a task is called directly.

The neutral base class is special because it's not bound to any specific app
yet.  Concrete subclasses of this class will be bound, so you should
always mark generic base classes as ``abstract``

Once a task is bound to an app it will read configuration to set default values
and so on.

It's also possible to change the default base class for an application
by changing its :meth:`@Task` attribute:

.. code-block:: python

    >>> from celery import Celery, Task

    >>> app = Celery()

    >>> class MyBaseTask(Task):
    ...    abstract = True
    ...    send_error_emails = True

    >>> app.Task = MyBaseTask
    >>> app.Task
    <unbound MyBaseTask>

    >>> @app.task
    ... def add(x, y):
    ...     return x + y

    >>> add
    <@task: __main__.add>

    >>> add.__class__.mro()
    [<class add of <Celery __main__:0x1012b4410>>,
     <unbound MyBaseTask>,
     <unbound Task>,
     <type 'object'>]
