=====================
 Broker Installation
=====================

Installing RabbitMQ
===================

See `Installing RabbitMQ`_ over at RabbitMQ's website. For Mac OS X
see `Installing RabbitMQ on OS X`_.

.. _`Installing RabbitMQ`: http://www.rabbitmq.com/install.html
.. _`Installing RabbitMQ on OS X`:
    http://playtype.net/past/2008/10/9/installing_rabbitmq_on_osx/

Setting up RabbitMQ
-------------------

To use celery we need to create a RabbitMQ user, a virtual host and
allow that user access to that virtual host::

    $ rabbitmqctl add_user myuser mypassword

    $ rabbitmqctl add_vhost myvhost

    $ rabbitmqctl set_permissions -p myvhost myuser "" ".*" ".*"

See the RabbitMQ `Admin Guide`_ for more information about `access control`_.

.. _`Admin Guide`: http://www.rabbitmq.com/admin-guide.html

.. _`access control`: http://www.rabbitmq.com/admin-guide.html#access-control
