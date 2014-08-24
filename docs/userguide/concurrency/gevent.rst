.. _concurrency-gevent:

===========================
 Concurrency with Gevent
===========================

.. _gevent-introduction:

Introduction
============

Gevent is a combined IO loop and event handling library that like eventlet
allows you to change how you run your code, not how you write it.
    * It uses `epool` _or `libev`_ for
      `highly scalable non-blocking I/O`_.
    * `Coroutines`_ ensure that the developer uses a blocking style of
      programming that is similar to threading, but provide the benefits of
      non-blocking I/O.
    * The event dispatch is implicit, which means you can easily use Gevent
      from the Python interpreter, or as a small part of a larger application.

Celery supports Eventlet as an alternative execution pool implementation.
It is in some cases superior to prefork, but you need to ensure
your tasks do not perform blocking calls, as this will halt all
other operations in the worker until the blocking call returns.

The prefork pool can take use of multiple processes, but how many is
often limited to a few processes per CPU.  With Gevent you can efficiently
spawn hundreds, or thousands of green threads.  In an informal test with a
feed hub system the Gevent pool could fetch and process hundreds of feeds
every second, while the prefork pool spent 14 seconds processing 100
feeds.  Note that is one of the applications evented I/O is especially good
at (asynchronous HTTP requests).  You may want a mix of both Eventlet and
prefork workers, and route tasks according to compatibility or
what works best.

Enabling Gevent
=================

You can enable the Gevent pool by using the ``-P`` option to
:program:`celery worker`:

.. code-block:: bash

    $ celery worker -P gevent -c 1000

Use of the Gevent pool will automatically apply monkey patching to the python standard functions.
If you are running with the django ORM and a greenlet safe thread pool it might be required to 
not to monkey patch the thread functions. That is possible by setting the environment variable
GEVENT_PATCHTHREAD to 'N'

.. code-block:: bash
    $ GEVENT_PATCHTHREAD='N' celery worker -P gevent -c 1000
    
.. _gevent-examples:

Examples
========

See the `Gevent examples`_ directory in the Celery distribution for
some examples taking use of Gevent support.

.. _`Gevent`: http://gevent.org
.. _`libev`: http://libev.schmorp.de
.. _`highly scalable non-blocking I/O`:
    http://en.wikipedia.org/wiki/Asynchronous_I/O#Select.28.2Fpoll.29_loops
.. _`Coroutines`: http://en.wikipedia.org/wiki/Coroutine
.. _`gevent examples`:
    https://github.com/celery/celery/tree/master/examples/gevent

