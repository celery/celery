.. _broker-couchdb:

===============
 Using CouchDB
===============

.. _broker-couchdb-installation:

Installation
============

For the CouchDB support you have to install additional dependencies.
You can install both Celery and these dependencies in one go using
either the `celery-with-couchdb`_, or the `django-celery-with-couchdb` bundles::

    $ pip install -U celery-with-couchdb

.. _`celery-with-couchdb`:
    http://pypi.python.org/pypi/celery-with-couchdb
.. _`django-celery-with-couchdb`:
    http://pypi.python.org/pypi/django-celery-with-couchdb

.. _broker-couchdb-configuration:

Configuration
=============

Configuration is easy, set the transport, and configure the location of
your CouchDB database::

    BROKER_URL = "couchdb://localhost:5984/database_name"

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

The Beanstalk message transport does not currently support:

    * Remote control commands (celeryctl, broadcast)
