.. _guide-webhooks:

================================
 HTTP Callback Tasks (Webhooks)
================================

.. module:: celery.task.http

.. contents::
    :local:

.. _webhook-basics:

Basics
======

If you need to call into another language, framework or similar, you can
do so by using HTTP callback tasks.

The HTTP callback tasks uses GET/POST data to pass arguments and returns
result as a JSON response. The scheme to call a task is:

.. code-block:: http

    GET /mytask/?arg1=a&arg2=b&arg3=c HTTP/1.1
    Host: example.com

or using POST:

.. code-block:: http

    POST /mytask HTTP/1.1
    Host: example.com

.. note::

    POST data needs to be form encoded.

Whether to use GET or POST is up to you and your requirements.

The web page should then return a response in the following format
if the execution was successful:

.. code-block:: json

    {"status": "success", "retval": "RETVAL"}

or if there was an error:

.. code-block:: json

    {"status": "failure", "reason": "Invalid moon alignment."}

Enabling the HTTP task
----------------------

To enable the HTTP dispatch task you have to add :mod:`celery.task.http`
to :setting:`imports`, or start the worker with
:option:`-I celery.task.http <celery worker -I>`.


.. _webhook-django-example:

Django webhook example
======================

With this information you could define a simple task in Django:

.. code-block:: python

    from django.http import HttpResponse
    from json import dumps


    def multiply(request):
        x = int(request.GET['x'])
        y = int(request.GET['y'])
        result = x * y
        response = {'status': 'success', 'retval': result}
        return HttpResponse(dumps(response), mimetype='application/json')

.. _webhook-rails-example:

Ruby on Rails webhook example
=============================

or in Ruby on Rails:

.. code-block:: ruby

    def multiply
        @x = params[:x].to_i
        @y = params[:y].to_i

        @status = {:status => 'success', :retval => @x * @y}

        render :json => @status
    end

You can easily port this scheme to any language/framework;
new examples and libraries are very welcome.

.. _webhook-calling:

Calling webhook tasks
=====================

To call a task you can use the :class:`~celery.task.http.URL` class:

.. code-block:: pycon

    >>> from celery.task.http import URL
    >>> res = URL('http://example.com/multiply').get_async(x=10, y=10)


:class:`~celery.task.http.URL` is a shortcut to the :class:`HttpDispatchTask`.
You can subclass this to extend the
functionality:

.. code-block:: pycon

    >>> from celery.task.http import HttpDispatchTask
    >>> res = HttpDispatchTask.delay(
    ...     url='http://example.com/multiply',
    ...     method='GET', x=10, y=10)
    >>> res.get()
    100

The output of :program:`celery worker` (or the log file if enabled) should show the
task being executed:

.. code-block:: text

    [INFO/MainProcess] Task celery.task.http.HttpDispatchTask
            [f2cc8efc-2a14-40cd-85ad-f1c77c94beeb] processed: 100

Since calling tasks can be done via HTTP using the
:func:`djcelery.views.apply` view, calling tasks from other languages is easy.
For an example service exposing tasks via HTTP you should have a look at
`examples/celery_http_gateway` in the Celery distribution:
https://github.com/celery/celery/tree/master/examples/celery_http_gateway/
