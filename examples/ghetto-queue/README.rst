=======================================================
 Example Celery project using a database message queue
=======================================================

Short instructions
==================

Quick rundown of the tutorial::

1. Install the `ghettoq`_ plugin.

    $ pip install ghettoq

    $ celeryinit

2. Open up two terminals. In the first, run:

    $ celeryd --loglevel=INFO

  In the second you run the test program:

    $ python ./test.py

Voila, you've executed some tasks!

Instructions
============

This example uses the database as a message queue (commonly called a "ghetto
queue"). Excellent for testing, but not suitable for production
installations.

To try it out you have to install the `GhettoQ`_ package first::

    $ pip install ghettoq

This package is an add-on to `Carrot`_; the messaging abstraction celery
uses. The add-on enables the use of databases as message queues. Currently it
supports `Redis`_ and relational databases via the Django ORM.

.. _`ghettoq`: http://pypi.python.org/pypi/ghettoq
.. _`Carrot`: http://pypi.python.org/pypi/carrot
.. _`Redis`: http://code.google.com/p/redis/


The provided ``celeryconfig.py`` configures the settings used to drive celery.

Next we have to create the database tables by issuing the ``celeryinit``
command::

    $ celeryinit

We're using SQLite3, so this creates a database file (``celery.db`` as
specified in the config file). SQLite is great, but when used in combination
with Django it doesn't handle concurrency well. To protect your program from
lock problems, celeryd will only spawn one worker process. With
other database drivers you can specify as many worker processes as you want.


With the setup done, let's run the worker::

    $ celeryd --loglevel=INFO


You should see the worker starting up. As it will continue running in
the foreground, we have to open up another terminal to run our test program::

    $ python test.py


The test program simply runs the ``add`` task, which is a simple task adding
numbers. You can also run the task manually if you want::

    >>> from tasks import add
    >>> result = add.delay(4, 4)
    >>> result.wait()
    8

Using Redis instead
===================

To use redis instead, you have to configure the following directives in 
``celeryconfig.py``::

    CARROT_BACKEND = "ghettoq.taproot.Redis"
    BROKER_HOST = "localhost"
    BROKER_PORT = 6379

Modules
=======

    * celeryconfig.py

        The celery configuration module.

    * tasks.py

        Tasks are defined in this module. This module is automatically
        imported by the worker because it's listed in
        celeryconfig's ``CELERY_IMPORTS`` directive.

    * test.py

        Simple test program running tasks.


More information
================

http://celeryproject.org
