.. _concurrency-eventlet:

===========================
 Concurrency with gevent
===========================

.. _gevent-introduction:

Introduction
============

The `gevent`_ homepage describes it a coroutine_ -based Python_ networking library that uses
`greenlet <https://greenlet.readthedocs.io>`_ to provide a high-level synchronous API on top of the `libev`_
or `libuv`_ event loop.

Features include:

* Fast event loop based on `libev`_ or `libuv`_.
* Lightweight execution units based on greenlets.
* API that re-uses concepts from the Python standard library (for
  examples there are `events`_ and
  `queues`_).
* `Cooperative sockets with SSL support <http://www.gevent.org/api/index.html#networking>`_
* `Cooperative DNS queries <http://www.gevent.org/dns.html>`_ performed through a threadpool,
  dnspython, or c-ares.
* `Monkey patching utility <http://www.gevent.org/intro.html#monkey-patching>`_ to get 3rd party modules to become cooperative
* TCP/UDP/HTTP servers
* Subprocess support (through `gevent.subprocess`_)
* Thread pools

gevent is `inspired by eventlet`_ but features a more consistent API,
simpler implementation and better performance. Read why others `use
gevent`_ and check out the list of the `open source projects based on
gevent`_.


Enabling gevent
=================

You can enable the gevent pool by using the
:option:`celery worker -P gevent` or  :option:`celery worker --pool=gevent`
worker option.

.. code-block:: console

    $ celery -A proj worker -P gevent -c 1000

.. _eventlet-examples:

Examples
========

See the `gevent examples`_ directory in the Celery distribution for
some examples taking use of Eventlet support.

Known issues
============
There is a known issue using python 3.11 and gevent.
The issue is documented `here`_ and addressed in a `gevent issue`_.
Upgrading to greenlet 3.0 solves it.

.. _events: http://www.gevent.org/api/gevent.event.html#gevent.event.Event
.. _queues: http://www.gevent.org/api/gevent.queue.html#gevent.queue.Queue
.. _`gevent`: http://www.gevent.org/
.. _`gevent examples`:
    https://github.com/celery/celery/tree/main/examples/gevent
.. _gevent.subprocess: http://www.gevent.org/api/gevent.subprocess.html#module-gevent.subprocess

.. _coroutine: https://en.wikipedia.org/wiki/Coroutine
.. _Python: http://python.org
.. _libev: http://software.schmorp.de/pkg/libev.html
.. _libuv: http://libuv.org
.. _inspired by eventlet: http://blog.gevent.org/2010/02/27/why-gevent/
.. _use gevent: http://groups.google.com/group/gevent/browse_thread/thread/4de9703e5dca8271
.. _open source projects based on gevent: https://github.com/gevent/gevent/wiki/Projects
.. _what's new: http://www.gevent.org/whatsnew_1_5.html
.. _changelog: http://www.gevent.org/changelog.html
.. _here: https://github.com/celery/celery/issues/8425
.. _gevent issue: https://github.com/gevent/gevent/issues/1985
