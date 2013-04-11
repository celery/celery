.. _broker-ironmq:

==================
 Using IronMQ
==================

.. _broker-ironmq-installation:

Installation
============

For IronMQ support, you'll need the [iron_celery](http://github.com/iron-io/iron_celery) library:

.. code-block:: bash

    $ pip install iron_celery

As well as an [Iron.io account](http://www.iron.io). Sign up for free at [iron.io](http://www.iron.io).

.. _broker-ironmq-configuration:

Configuration
=============

First, you'll need to import the iron_celery library right after you import Celery, for example::

    from celery import Celery
    import iron_celery

    celery = Celery('mytasks', broker='ironmq://', backend='ironcache://')

You have to specify IronMQ in the broker URL::

    BROKER_URL = 'ironmq://ABCDEFGHIJKLMNOPQRST:ZYXK7NiynGlTogH8Nj+P9nlE73sq3@'

where the URL format is::

    ironmq://project_id:token@

you must *remember to include the "@" at the end*.

The login credentials can also be set using the environment variables
:envvar:`IRON_TOKEN` and :envvar:`IRON_PROJECT_ID`, which are set automatically if you use the IronMQ Heroku add-on.
And in this case the broker url may only be::

    ironmq://

Clouds
------

The default cloud/region is ``AWS us-east-1``. You can choose the IronMQ Rackspace cloud by changing the URL to::

    ironmq://project_id:token@mq-rackspace-dfw.iron.io

Results
=======

You can store results in IronCache with the same Iron.io credentials, just set the results URL with the same syntax
as the broker URL, but changing the start to ``ironcache``::

    ironcache:://project_id:token@

This will default to a cache named "Celery", if you want to change that::

    ironcache:://project_id:token@/awesomecache

More Information
================

You can find more information in the [iron_celery README](http://github.com/iron-io/iron_celery).
