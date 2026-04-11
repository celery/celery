==================================
  Example using the gevent Pool
==================================

Introduction
============

This is a Celery application containing two example tasks.

First you need to install gevent::

    $ python -m pip install gevent celery pybloom-live

Before you run any of the example tasks you need to start
the worker::

    $ cd examples/gevent
    $ celery worker -l INFO --concurrency=500 --pool=gevent

As usual you need to have RabbitMQ running, see the Celery getting started
guide if you haven't installed it yet.

Tasks
=====

* `tasks.urlopen`

This task simply makes a request opening the URL and returns the size
of the response body::

    $ cd examples/gevent
    $ python
    >>> from tasks import urlopen
    >>> urlopen.delay('https://www.google.com/').get()
    9980

To open several URLs at once you can do::

    $ cd examples/gevent
    $ python
    >>> from tasks import urlopen
    >>> from celery import group
    >>> result = group(urlopen.s(url)
    ...                     for url in LIST_OF_URLS).apply_async()
    >>> for incoming_result in result.iter_native():
    ...     print(incoming_result)


This is a simple recursive web crawler.  It will only crawl
URLs for the current host name.  Please see comments in the
`webcrawler.py` file.
