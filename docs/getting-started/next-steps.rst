============
 Next Steps
============

The :ref:`first-steps` guide is intentionally minimal.  In this guide
we will demonstrate what Celery offers in more detail, including
how to add Celery support for your application and library.


Our Project
===========

Project layout::

    proj/__init__.py
        /celery.py
        /tasks.py

:file:`proj/celery.py`

.. literalinclude:: ../../examples/next-steps/proj/celery.py
    :language python:


:file:`proj/tasks.py`

.. literalinclude:: ../../examples/next-steps/proj/tasks.py
    :language python:



Subtasks
========


group
-----

.. code-block:: python

    >>> from celery import group
    >>> from proj.tasks import add

    >>> ~group(add.s(i, i) for i in xrange(10))
    [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

    >>> group(add.s(i, i) for i in xrange(10)).skew(1, 10)

map/starmap
-------------

.. code-block:: python

    >>> from proj.tasks import add

    >>> ~xsum.map([range(10), range(100)])
    [45, 4950]

    >>> ~add.starmap(zip(range(10), range(10)))
    [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

    >>> add.starmap(zip(range(10), range(10))).apply_async(countdown=10)

chunks
------

.. code-block:: python

    >>> from proj.tasks import add

    >>> ~add.chunks(zip(range(100), range(100)), 10)



