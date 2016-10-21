====================================
 ``celery.utils.debug``
====================================

.. contents::
    :local:

Sampling Memory Usage
=====================

This module can be used to diagnose and sample the memory usage
used by parts of your application.

For example, to sample the memory usage of calling tasks you can do this:

.. code-block:: python


    from celery.utils.debug import sample_mem, memdump

    from tasks import add


    try:
        for i in range(100):
            for j in range(100):
                add.delay(i, j)
            sample_mem()
    finally:
        memdump()


API Reference
=============

.. currentmodule:: celery.utils.debug

.. automodule:: celery.utils.debug

    .. autofunction:: sample_mem

    .. autofunction:: memdump

    .. autofunction:: sample

    .. autofunction:: mem_rss

    .. autofunction:: ps
