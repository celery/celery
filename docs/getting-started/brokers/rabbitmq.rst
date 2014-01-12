.. _broker-rabbitmq:

================
 Using RabbitMQ
================

.. contents::
    :local:

Installation & Configuration
============================

RabbitMQ is the default broker so it does not require any additional
dependencies or initial configuration, other than the URL location of
the broker instance you want to use::

    >>> BROKER_URL = 'amqp://guest:guest@localhost:5672//'

For a description of broker URLs and a full list of the
various broker configuration options available to Celery,
see :ref:`conf-broker-settings`.

.. _installing-rabbitmq:

Installing the RabbitMQ Server
==============================

See `Installing RabbitMQ`_ over at RabbitMQ's website. For Mac OS X
see `Installing RabbitMQ on OS X`_.

.. _`Installing RabbitMQ`: http://www.rabbitmq.com/install.html

.. note::

    If you're getting `nodedown` errors after installing and using
    :program:`rabbitmqctl` then this blog post can help you identify
    the source of the problem:

        http://somic.org/2009/02/19/on-rabbitmqctl-and-badrpcnodedown/

.. _rabbitmq-configuration:

Setting up RabbitMQ
-------------------

To use celery we need to create a RabbitMQ user, a virtual host and
allow that user access to that virtual host:

.. code-block:: bash

    $ sudo rabbitmqctl add_user myuser mypassword

.. code-block:: bash

    $ sudo rabbitmqctl add_vhost myvhost

.. code-block:: bash

    $ sudo rabbitmqctl set_permissions -p myvhost myuser ".*" ".*" ".*"

See the RabbitMQ `Admin Guide`_ for more information about `access control`_.

.. _`Admin Guide`: http://www.rabbitmq.com/admin-guide.html

.. _`access control`: http://www.rabbitmq.com/admin-guide.html#access-control

.. _rabbitmq-osx-installation:

Installing RabbitMQ on OS X
---------------------------

The easiest way to install RabbitMQ on OS X is using `Homebrew`_ the new and
shiny package management system for OS X.

First, install homebrew using the one-line command provided by the `Homebrew
documentation`_:

.. code-block:: bash

    ruby -e "$(curl -fsSL https://raw.github.com/Homebrew/homebrew/go/install)"

Finally, we can install rabbitmq using :program:`brew`:

.. code-block:: bash

    $ brew install rabbitmq

.. _`Homebrew`: http://github.com/mxcl/homebrew/
.. _`Homebrew documentation`: https://github.com/Homebrew/homebrew/wiki/Installation

.. _rabbitmq-osx-system-hostname:

Configuring the system host name
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you're using a DHCP server that is giving you a random host name, you need
to permanently configure the host name. This is because RabbitMQ uses the host name
to communicate with nodes.

Use the :program:`scutil` command to permanently set your host name:

.. code-block:: bash

    $ sudo scutil --set HostName myhost.local

Then add that host name to :file:`/etc/hosts` so it's possible to resolve it
back into an IP address::

    127.0.0.1       localhost myhost myhost.local

If you start the rabbitmq server, your rabbit node should now be `rabbit@myhost`,
as verified by :program:`rabbitmqctl`:

.. code-block:: bash

    $ sudo rabbitmqctl status
    Status of node rabbit@myhost ...
    [{running_applications,[{rabbit,"RabbitMQ","1.7.1"},
                        {mnesia,"MNESIA  CXC 138 12","4.4.12"},
                        {os_mon,"CPO  CXC 138 46","2.2.4"},
                        {sasl,"SASL  CXC 138 11","2.1.8"},
                        {stdlib,"ERTS  CXC 138 10","1.16.4"},
                        {kernel,"ERTS  CXC 138 10","2.13.4"}]},
    {nodes,[rabbit@myhost]},
    {running_nodes,[rabbit@myhost]}]
    ...done.

This is especially important if your DHCP server gives you a host name
starting with an IP address, (e.g. `23.10.112.31.comcast.net`), because
then RabbitMQ will try to use `rabbit@23`, which is an illegal host name.

.. _rabbitmq-osx-start-stop:

Starting/Stopping the RabbitMQ server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To start the server:

.. code-block:: bash

    $ sudo rabbitmq-server

you can also run it in the background by adding the :option:`-detached` option
(note: only one dash):

.. code-block:: bash

    $ sudo rabbitmq-server -detached

Never use :program:`kill` to stop the RabbitMQ server, but rather use the
:program:`rabbitmqctl` command:

.. code-block:: bash

    $ sudo rabbitmqctl stop

When the server is running, you can continue reading `Setting up RabbitMQ`_.
