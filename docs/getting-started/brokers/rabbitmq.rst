.. _broker-rabbitmq:

================
 Using RabbitMQ
================

.. contents::
    :local:

Installation & Configuration
============================

RabbitMQ is the default broker so it doesn't require any additional
dependencies or initial configuration, other than the URL location of
the broker instance you want to use:

.. code-block:: python

    broker_url = 'amqp://guest:guest@localhost:5672//'

For a description of broker URLs and a full list of the
various broker configuration options available to Celery,
see :ref:`conf-broker-settings`.

.. _installing-rabbitmq:

Installing the RabbitMQ Server
==============================

See `Installing RabbitMQ`_ over at RabbitMQ's website. For macOS
see `Installing RabbitMQ on macOS`_.

.. _`Installing RabbitMQ`: http://www.rabbitmq.com/install.html

.. note::

    If you're getting `nodedown` errors after installing and using
    :command:`rabbitmqctl` then this blog post can help you identify
    the source of the problem:

        http://somic.org/2009/02/19/on-rabbitmqctl-and-badrpcnodedown/

.. _rabbitmq-configuration:

Setting up RabbitMQ
-------------------

To use Celery we need to create a RabbitMQ user, a virtual host and
allow that user access to that virtual host:

.. code-block:: console

    $ sudo rabbitmqctl add_user myuser mypassword

.. code-block:: console

    $ sudo rabbitmqctl add_vhost myvhost

.. code-block:: console

    $ sudo rabbitmqctl set_user_tags myuser mytag

.. code-block:: console

    $ sudo rabbitmqctl set_permissions -p myvhost myuser ".*" ".*" ".*"

See the RabbitMQ `Admin Guide`_ for more information about `access control`_.

.. _`Admin Guide`: http://www.rabbitmq.com/admin-guide.html

.. _`access control`: http://www.rabbitmq.com/admin-guide.html#access-control

.. _rabbitmq-macOS-installation:

Installing RabbitMQ on macOS
----------------------------

The easiest way to install RabbitMQ on macOS is using `Homebrew`_ the new and
shiny package management system for macOS.

First, install Homebrew using the one-line command provided by the `Homebrew
documentation`_:

.. code-block:: console

    ruby -e "$(curl -fsSL https://raw.github.com/Homebrew/homebrew/go/install)"

Finally, we can install RabbitMQ using :command:`brew`:

.. code-block:: console

    $ brew install rabbitmq

.. _`Homebrew`: https://github.com/mxcl/homebrew/
.. _`Homebrew documentation`: https://github.com/Homebrew/homebrew/wiki/Installation

.. _rabbitmq-macOS-system-hostname:

After you've installed RabbitMQ with :command:`brew` you need to add the following to
your path to be able to start and stop the broker: add it to the start-up file for your
shell (e.g., :file:`.bash_profile` or :file:`.profile`).

.. code-block:: bash

    PATH=$PATH:/usr/local/sbin

Configuring the system host name
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you're using a DHCP server that's giving you a random host name, you need
to permanently configure the host name. This is because RabbitMQ uses the host name
to communicate with nodes.

Use the :command:`scutil` command to permanently set your host name:

.. code-block:: console

    $ sudo scutil --set HostName myhost.local

Then add that host name to :file:`/etc/hosts` so it's possible to resolve it
back into an IP address::

    127.0.0.1       localhost myhost myhost.local

If you start the :command:`rabbitmq-server`, your rabbit node should now
be `rabbit@myhost`, as verified by :command:`rabbitmqctl`:

.. code-block:: console

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
starting with an IP address, (e.g., `23.10.112.31.comcast.net`).  In this
case RabbitMQ will try to use `rabbit@23`: an illegal host name.

.. _rabbitmq-macOS-start-stop:

Starting/Stopping the RabbitMQ server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To start the server:

.. code-block:: console

    $ sudo rabbitmq-server

you can also run it in the background by adding the ``-detached`` option
(note: only one dash):

.. code-block:: console

    $ sudo rabbitmq-server -detached

Never use :command:`kill` (:manpage:`kill(1)`) to stop the RabbitMQ server,
but rather use the :command:`rabbitmqctl` command:

.. code-block:: console

    $ sudo rabbitmqctl stop

When the server is running, you can continue reading `Setting up RabbitMQ`_.
