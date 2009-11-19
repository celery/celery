==============
 Remote Tasks 
==============

.. module:: celery.task.rest

Executing tasks on a remote web server
--------------------------------------

If you need to call into another language, framework or similar, you can
do so by using HTTP tasks.

The HTTP tasks (or REST task) uses a simple REST+JSON scheme to take arguments
and return results, the scheme to call a task is::

    GET http://example.com/mytask/?arg1=a,arg2=b,arg3=c

The web page should then return a response in the following format
if the execution was successful::

    {"status": "success", "retval": ....}

or in the following format if there was an error::

    {"status": "failure": "reason": "Invalid moon alignment."}


With this information we can define a simple task in Django:

.. code-block:: python

    from django.http import HttpResponse
    from anyjson import serialize


    def multiply(request):
        x = int(request.GET["x"])
        y = int(request.GET["y"])
        result = x * y
        response = {"status": "success", "retval": result}
        return HttpResponse(serialize(response), mimetype="application/json")

I'm sure you'll be able to port this scheme to any language/framework.
New examples and libraries are very welcome!

To execute the task you use :class:`RESTProxyTask`:

    >>> from celery.task import RESTProxyTask
    >>> res = RESTProxyTask.delay("http://example.com/multiply", x=10, y=10)
    >>> res.get()
    100

In your ``celeryd.log`` file you should see the task being processed::

    [INFO/MainProcess] Task celery.task.rest.RESTProxyTask
        [f2cc8efc-2a14-40cd-85ad-f1c77c94beeb] processed: 100

Since applying tasks can also simply be done via the web and the
``celery.views.apply`` view, executing tasks from other languages should be a
no-brainer.
