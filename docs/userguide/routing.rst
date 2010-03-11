===============
 Routing Tasks
===============

**NOTE** This document refers to functionality only available in brokers
using AMQP. Other brokers may implement some functionality, see their
respective documenation for more information, or contact the `mailinglist`_.

.. _`mailinglist`: http://groups.google.com/group/celery-users

AMQP Primer
===========

Exchanges, queues and routing keys.
-----------------------------------
TODO Mindblowing one-line simple explanation here. TODO

Exchange type
-------------

The exchange type defines how the messages are routed through the exchange.
The exchanges defined in the standard is ``direct``, ``topic``, ``fanout`` and
``headers``. Also non-standard exchange types available as plugins to RabbitMQ, like
the last value cache plug-in.


Consumers and Producers
-----------------------
TODO

Related API commands
-------------------------

* exchange.declare(exchange_name, type, passive, durable, auto_delete, internal)

Declares an exchange by name.

    * ``passive`` means the exchange won't be created, but you can use this to
      check if the exchange already exists.

    * Durable exchanges are persistent. I.e. they survive a broker restart.

    * ``auto_delete`` means the queue will be deleted by the broker when there
      are no more queues using it.

* queue.declare(queue_name, passive, durable, exclusive, auto_delete)

Declares a queue by name.

    * exclusive queues can only be consumed from by the current connection.
      implies ``auto_delete``.

* queue.bind(queue_name, exchange_name, routing_key)

Binds a queue to an exchange with a routing key.
Unbound queues will not receive messages, so this is necessary.

* queue.delete(name, if_unused, if_empty)

Deletes a queue and its binding.

* exchange.delete(name, if_unused)

Deletes an exchange.

**NOTE**: Declaring does not necessarily mean "create". When you declare you
*assert* that the entity exists and it is operable. There is no rule as to
whom should initially create the exchange/queue/binding, whether consumer
or producer.  Usually the first one to need it will create it.


Hands-on with the API
---------------------

Celery comes with a tool called ``camqadm`` (short for celery AMQP admin).
It's used for simple admnistration tasks like deleting queues/exchanges,
purging queues and creating queue entities. In short it's for simple command
line access to the AMQP API.

