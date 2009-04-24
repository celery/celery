============================================
crunchy - Distributed Task Queue for Django.
============================================

:Authors:
    Ask Solem (askh@opera.com)
:Version: 0.1.0

Introduction
------------

``crunchy`` is a distributed task queue framework for Django.
More information will follow.

Installation
=============

You can install ``crunchy`` either via the Python Package Index (PyPI)
or from source.

To install using ``pip``,::

    $ pip install crunchy

To install using ``easy_install``,::

    $ easy_install crunchy

If you have downloaded a source tarball you can install it
by doing the following,::

    $ python setup.py build
    # python setup.py install # as root

Usage
=====

Have to write a cool tutorial, but here is some simple usage info.

*Note* You need to have a AMQP message broker running, like `RabbitMQ`_,
and you need to have the amqp server setup in your settings file, as described
in the `carrot distribution README`_.

*Note* If you're running ``SQLite`` as the database backend, ``crunchd`` will
only be able to process one message at a time, this because ``SQLite`` doesn't
allow concurrent writes.

.. _`RabbitMQ`: http://www.rabbitmq.com
.. _`carrot distribution README`: http://pypi.python.org/pypi/carrot/0.3.3


Defining tasks
--------------

    >>> from crunchy.task import tasks
    >>> from crunchy.log import setup_logger
    >>> def do_something(some_arg, **kwargs):
    ...     logger = setup_logger(**kwargs)
    ...     logger.info("Did something: %s" % some_arg)
    >>> task.register("do_something", some_arg=do_something) 

*Note* Task functions only supports keyword arguments.

Tell the crunch daemon to run a task
-------------------------------------

    >>> from crunchy.task import delay_task
    >>> delay_task("do_something", "foo bar baz")


Running the crunch daemon
--------------------------

::

    $ cd mydjangoproject
    $ env DJANGO_SETTINGS_MODULE=settings crunchd
    [....]
    [2009-04-23 17:44:05,115: INFO/Process-1] Did something: foo bar baz
    [2009-04-23 17:44:05,118: INFO/MainProcess] Waiting for queue.




Autodiscovery of tasks
-----------------------

``crunchy`` has an autodiscovery feature like the Django Admin, that
automatically loads any ``tasks.py`` module in the applications listed
in ``settings.INSTALLED_APPS``.

A good place to add this command could be in your ``urls.py``,
::

    from crunchy.task import tasks
    tasks.autodiscover()



Then you can add new tasks in your applications ``tasks.py`` module,
::

    from crunchy.task import tasks
    from crunchy.log import setup_logger
    from clickcounter.models import ClickCount

    def increment_click(for_url, **kwargs):
        logger = setup_logger(**kwargs)
        clicks_for_url, cr = ClickCount.objects.get_or_create(url=for_url)
        clicks_for_url.clicks = clicks_for_url.clicks + 1
        clicks_for_url.save()
        logger.info("Incremented click count for %s (not at %d)" % (
                        for_url, clicks_for_url.clicks)
    tasks.register("increment_click", increment_click)

License
=======

This software is licensed under the ``New BSD License``. See the ``LICENSE``
file in the top distribution directory for the full license text.

.. # vim: syntax=rst expandtab tabstop=4 shiftwidth=4 shiftround
