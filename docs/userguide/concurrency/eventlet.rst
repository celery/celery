.. _concurrency-eventlet:

===========================
 Concurrency with Eventlet
===========================

.. _eventlet-introduction:

Introduction
============

The `Eventlet`_ homepage describes eventlet as,
a concurrent networking library for Python that allows you to
change how you run your code, not how you write it.

    * It uses epoll or libevent for `highly scalable non-blocking I/O`_.
    * `Coroutines`_ ensure that the developer uses a blocking style of
      programming that is similar to threading, but provide the benefits of
      non-blocking I/O.
    * The event dispatch is implicit, which means you can easily use Eventlet
      from the Python interpreter, or as a small part of a larger application.

Celery supports Eventlet as an alternative execution pool implementation.
It is in some cases superior to multiprocessing, but you need to ensure
your tasks do not perform any blocking calls, as this will halt all
other operations in the worker.

The multiprocessing pool can take use of many processes, but it is often
limited to a few processes per CPU.  With eventlet you can efficiently spawn
hundreds of concurrent couroutines.  In an informal test with a feed hub
system the Eventlet pool could fetch and process hundreds of feeds every
second, while the multiprocessing pool spent 14 seconds processing 100 feeds.
But this is exactly the kind of application evented I/O is good for.  
You may want a a mix of both eventlet and multiprocessing workers,
depending on the needs of your tasks.

Enabling Eventlet
=================

You can enable the Eventlet pool by using the `-P` option to
:program:`celeryd`::

    $ celeryd -P eventlet -c 1000

.. _eventlet-examples:

Examples
========

See the `Eventlet examples`_ directory in the Celery distribution for
some examples taking use of Eventlet support.



.. _`Eventlet`: http://eventlet.net
.. _`highly scalable non-blocking I/O`:
    http://en.wikipedia.org/wiki/Asynchronous_I/O#Select.28.2Fpoll.29_loops
.. _`Coroutines`: http://en.wikipedia.org/wiki/Coroutine
.. _`Eventlet examples`:
    https://github.com/ask/celery/tree/master/examples/eventlet

