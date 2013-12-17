.. _broker-mongodb:

===============
 Using MongoDB
===============

.. admonition:: Experimental Status

    The MongoDB transport is in need of improvements in many areas and there
    are several open bugs.  Sadly we don't have the resources or funds
    required to improve the situation, so we're looking for contributors
    and partners willing to help.

.. _broker-mongodb-installation:

Installation
============

For the MongoDB support you have to install additional dependencies.
You can install both Celery and these dependencies in one go using
the ``celery[mongodb]`` :ref:`bundle <bundles>`:

.. code-block:: bash

    $ pip install -U celery[mongodb]

.. _broker-mongodb-configuration:

Configuration
=============

Configuration is easy, set the transport, and configure the location of
your MongoDB database::

    BROKER_URL = 'mongodb://localhost:27017/database_name'

Where the URL is in the format of::

    mongodb://userid:password@hostname:port/database_name

The host name will default to ``localhost`` and the port to 27017,
and so they are optional.  userid and password are also optional,
but needed if your MongoDB server requires authentication.

.. _mongodb-results-configuration:

Results
-------

If you also want to store the state and return values of tasks in MongoDB,
you should see :ref:`conf-mongodb-result-backend`.
