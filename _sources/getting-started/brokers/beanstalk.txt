.. _broker-beanstalk:

=================
 Using Beanstalk
=================

.. _broker-beanstalk-installation:

Installation
============

For the Beanstalk support you have to install additional dependencies.
You can install both Celery and these dependencies in one go using
either the `celery-with-beanstalk`_, or the `django-celery-with-beanstalk`
bundles::

    $ pip install -U celery-with-beanstalk

.. _`celery-with-beanstalk`:
    http://pypi.python.org/pypi/celery-with-beanstalk
.. _`django-celery-with-beanstalk`:
    http://pypi.python.org/pypi/django-celery-with-beanstalk

.. _broker-beanstalk-configuration:

Configuration
=============

Configuration is easy, set the transport, and configure the location of
your CouchDB database::

    BROKER_URL = "beanstalk://localhost:11300"

Where the URL is in the format of::

    beanstalk://hostname:port

The host name will default to ``localhost`` and the port to 11300,
and so they are optional.

.. _beanstalk-results-configuration:

Results
-------

Using Beanstalk to store task state and results is currently **not supported**.

.. _broker-beanstalk-limitations:

Limitations
===========

The Beanstalk message transport does not currently support:

    * Remote control commands (celeryctl, broadcast)
    * Authentication

