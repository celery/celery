.. _broker-ironmq:

==================
 Using IronMQ
==================

.. _broker-ironmq-installation:

Installation
============

For IronMQ support, you'll need the :pypi:`iron_celery` library:

.. code-block:: console

    $ pip install iron_celery

As well as an `Iron.io account <Iron.io>`_. Sign up for free at `Iron.io`_.


_`Iron.io`: http://www.iron.io/

.. _broker-ironmq-configuration:

Configuration
=============

First, you'll need to import the iron_celery library right after you
import Celery, for example:

.. code-block:: python

    from celery import Celery
    import iron_celery

    app = Celery('mytasks', broker='ironmq://', backend='ironcache://')

You have to specify IronMQ in the broker URL:

.. code-block:: python

    broker_url = 'ironmq://ABCDEFGHIJKLMNOPQRST:ZYXK7NiynGlTogH8Nj+P9nlE73sq3@'

where the URL format is:

.. code-block:: text

    ironmq://project_id:token@

you must *remember to include the "@" at the end*.

The login credentials can also be set using the environment variables
:envvar:`IRON_TOKEN` and :envvar:`IRON_PROJECT_ID`, which are set automatically
if you use the IronMQ Heroku add-on.  And in this case the broker URL may only be:

.. code-block:: text

    ironmq://

Clouds
------

The default cloud/region is ``AWS us-east-1``. You can choose the IronMQ Rackspace (ORD)
cloud by changing the URL to:

.. code-block:: text

    ironmq://project_id:token@mq-rackspace-ord.iron.io

Results
=======

You can store results in IronCache with the same ``Iron.io`` credentials,
just set the results URL with the same syntax
as the broker URL, but changing the start to ``ironcache``:

.. code-block:: text

    ironcache:://project_id:token@

This will default to a cache named "Celery", if you want to change that:

.. code-block:: text

    ironcache:://project_id:token@/awesomecache

More Information
================

You can find more information in the `iron_celery README`_.

_`iron_celery README`: https://github.com/iron-io/iron_celery/
