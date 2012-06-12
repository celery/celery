.. _guide-app:

=============
 Application
=============

The Celery library must instantiated before use, and this instance
is called the application.

Multiple Celery applications with different configuration
and components can co-exist in the same process space,
but there is always an app exposed as the 'current app' in each
thread.

Configuration
=============

There are lots of different options you can set that will change how
Celery work.  These options can be set on the app instance directly,
or you can use a dedicated configuration module.


The current configuration is always available as :attr:`@Celery.conf`::

    >>> celery.conf.CELERY_TIMEZONE
    "Europe/London"

The configuration actually consists of multiple dictionaries
that are consulted in order:

    #. Changes made at runtime.
    #. Any configuration module (``loader.conf``).
    #. The default configuration (:mod:`celery.app.defaults`).

When the app is serialized
only the configuration changes will be transferred.

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



.. topic:: Evolving the API

    Celery has changed a lot in the 3 years since it was initially
    created.

    For example, in the beginning it was possible to use any callable as
    a task::

    .. code-block:: python

        def hello(to):
            return "hello %s" % to

        >>> from celery.execute import apply_async

        >>> apply_async(hello, ("world!", ))

    or you could also create a ``Task`` class to set
    certain options, or override other behavior

    .. code-block:: python

        from celery.task import Task
        from celery.registry import tasks

        class Hello(Task):
            send_error_emails = True

            def run(self, to):
                return "hello %s" % to
        tasks.register(Hello)

        >>> Hello.delay("world!")

    Later, it was decided that passing arbitrary call-ables
    was an anti-pattern, since it makes it very hard to use
    serializers other than pickle, and the feature was removed
    in 2.0, replaced by task decorators::

    .. code-block:: python

        from celery.task import task

        @task(send_error_emails=True)
        def hello(x):
            return "hello %s" % to
