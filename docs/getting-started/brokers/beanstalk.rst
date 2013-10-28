.. _broker-beanstalk:

=================
 Using Beanstalk
=================

.. _broker-beanstalk-installation:

.. admonition:: Out of order

    The Beanstalk transport is currently not working well.

    We are interested in contributions and donations that can go towards
    improving this situation.



Installation
============

For the Beanstalk support you have to install additional dependencies.
You can install both Celery and these dependencies in one go using
the ``celery[beanstalk]`` :ref:`bundle <bundles>`:

.. code-block:: bash

    $ pip install -U celery[beanstalk]

.. _broker-beanstalk-configuration:

Configuration
=============

Configuration is easy, set the transport, and configure the location of
your Beanstalk database::

    BROKER_URL = 'beanstalk://localhost:11300'

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

    * Remote control commands (:program:`celery control`,
      :program:`celery inspect`, broadcast)
    * Authentication

