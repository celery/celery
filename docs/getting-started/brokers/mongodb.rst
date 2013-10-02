.. _broker-mongodb:

===============
 Using MongoDB
===============

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
