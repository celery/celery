.. _broker-couchdb:

===============
 Using CouchDB
===============

.. admonition:: Experimental Status

    The CouchDB transport is in need of improvements in many areas and there
    are several open bugs.  Sadly we don't have the resources or funds
    required to improve the situation, so we're looking for contributors
    and partners willing to help.

.. _broker-couchdb-installation:

Installation
============

For the CouchDB support you have to install additional dependencies.
You can install both Celery and these dependencies in one go using
the ``celery[couchdb]`` :ref:`bundle <bundles>`:

.. code-block:: bash

    $ pip install -U celery[couchdb]

.. _broker-couchdb-configuration:

Configuration
=============

Configuration is easy, set the transport, and configure the location of
your CouchDB database::

    BROKER_URL = 'couchdb://localhost:5984/database_name'

Where the URL is in the format of::

    couchdb://userid:password@hostname:port/database_name

The host name will default to ``localhost`` and the port to 5984,
and so they are optional.  userid and password are also optional,
but needed if your CouchDB server requires authentication.

.. _couchdb-results-configuration:

Results
-------

Storing task state and results in CouchDB is currently **not supported**.

.. _broker-couchdb-limitations:

Limitations
===========

The CouchDB message transport does not currently support:

    * Remote control commands (:program:`celery inspect`,
      :program:`celery control`, broadcast)
