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

The steps required to send and receive messages are:

    1. Create an exchange
    2. Create a queue
    3. Bind the queue to the exchange.

Exchange type
-------------

The exchange type defines how the messages are routed through the exchange.
The exchange types defined in the standard are ``direct``, ``topic``,
``fanout`` and ``headers``. Also non-standard exchange types are available
as plugins to RabbitMQ, like the ``last-value-cache plug-in`` by Michael
Bridgen. 

.. _`last-value-cache plug-in``:
    http://github.com/squaremo/rabbitmq-lvc-plugin


Consumers and Producers
-----------------------
TODO

Related API commands
--------------------

exchange.declare(exchange_name, type, passive, durable, auto_delete, internal)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Declares an exchange by name.

* ``passive`` means the exchange won't be created, but you can use this to
  check if the exchange already exists.

* Durable exchanges are persistent. That is - they survive a broker restart.

* ``auto_delete`` means the queue will be deleted by the broker when there
  are no more queues using it.

* queue.declare(queue_name, passive, durable, exclusive, auto_delete)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Declares a queue by name.

* exclusive queues can only be consumed from by the current connection.
  implies ``auto_delete``.

queue.bind(queue_name, exchange_name, routing_key)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Binds a queue to an exchange with a routing key.
Unbound queues will not receive messages, so this is necessary.

queue.delete(name, if_unused, if_empty)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Deletes a queue and its binding.

exchange.delete(name, if_unused)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Deletes an exchange.

**NOTE**: Declaring does not necessarily mean "create". When you declare you
*assert* that the entity exists and that it's operable. There is no rule as to
whom should initially create the exchange/queue/binding, whether consumer
or producer. Usually the first one to need it will be the one to create it.

Hands-on with the API
---------------------

Celery comes with a tool called ``camqadm`` (short for celery AMQP admin).
It's used for simple admnistration tasks like creating/deleting queues and
exchanges, purging queues and sending messages. In short it's for simple
command-line access to the AMQP API.

You can write commands directly in the arguments to ``camqadm``, or just start
with no arguments to start it in shell-mode::

    $ camqadm
    -> connecting to amqp://guest@localhost:5672/.
    -> connected.
    -->

Here ``-->`` is the prompt. Type ``help`` for a list of commands, there's
also autocomplete so you can start typing a command then hit ``tab`` to show a
list of possible matches.

Now let's create a queue we can send messages to::

    --> exchange.declare testexchange direct
    ok.
    --> queue.declare testqueue
    ok. queue:testqueue messages:0 consumers:0.
    --> queue.bind testqueue testexchange testkey
    ok.

This created the direct exchange ``testexchange``, and a queue
named ``testqueue``.  The queue is bound to the exchange using
the routing key ``testkey``.

From now on all messages sent to the exchange ``testexchange`` with routing
key ``testkey`` will be moved to this queue. We can send a message by
using the ``basic.publish`` command::

    --> basic.publish "This is a message!" testexchange testkey
    ok.


Now that the message is sent we can retrieve it again. We use the
``basic.get`` command here, which pops a single message off the queue,
this command is not recommended for production as it implies polling, any
real application would declare consumers instead.

Pop a message off the queue::

    --> basic.get testqueue
    {'body': 'This is a message!',
     'delivery_info': {'delivery_tag': 1,
                       'exchange': u'testexchange',
                       'message_count': 0,
                       'redelivered': False,
                       'routing_key': u'testkey'},
     'properties': {}}


AMQP uses acknowledgment to signify that a message has been received
and processed successfully. The message is sent to the next receiver
if it has not been acknowledged before the client connection is closed.

Note the delivery tag listed in the structure above; Within a connection channel,
every received message has a unique delivery tag,
This tag is used to acknowledge the message. Note that
delivery tags are not unique across connections, so in another client
the delivery tag ``1`` might point to a different message than in this channel.

You can acknowledge the message we received using ``basic.ack``::

    --> basic.ack 1
    ok.

To clean up after our test session we should delete the entities we created::

    --> queue.delete testqueue
    ok. 0 messages deleted.
    --> exchange.delete testexchange
    ok.
