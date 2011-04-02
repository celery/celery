======================
 Webhook Task Example
======================

This example is a simple Django HTTP service exposing a single task
multiplying two numbers:

The multiply http callback task is in `views.py`, mapped to a URL using
`urls.py`.

There are no models, so to start it do::

    $ python manage.py runserver

To execute the task you could use curl::

    $ curl http://localhost:8000/multiply?x=10&y=10

which then gives the expected JSON response::

    {"status": "success": "retval": 100}


To execute this http callback task asynchronously you could fire up
a python shell with a properly configured celery and do:

    >>> from celery.task.http import URL
    >>> res = URL("http://localhost:8000/multiply").get_async(x=10, y=10)
    >>> res.wait()
    100


That's all!
